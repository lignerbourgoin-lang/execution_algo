"""
Lottery Queue Selector for Multi-IP Waiting Rooms
-------------------------------------------------
Collects, ranks, and filters lottery queue numbers ("tombola") assigned to
multiple IP addresses or sessions in virtual waiting rooms (Queue-It, Ticketmaster, AXS).
Keeps the best queue positions (lowest ranks) and discards non-viable connections.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging
import threading
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

from core.network.ip_pool import SubnetIpPool

logger = logging.getLogger("execution.retail.tickets.lottery")


# [FEATURE: LOTTERY_QUEUE_SELECTOR] Multi-IP waiting room lottery position tracker and top-K selector
# Raison: In virtual waiting rooms, queue positions assigned to IPs are random; keeping the lowest queue numbers maximizes admission probability
# Attention: In queue lotteries, lower position number is better (rank 1 beats rank 1000)
@dataclass
class LotteryTicket:
    """Represents a queue position / lottery ticket assigned to an IP address."""

    ip_address: str
    queue_number: int
    session_id: str = ""
    status: str = "waiting"  # "waiting", "selected", "discarded", "admitted"
    received_at_timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def identifier(self) -> str:
        """Returns unique composite key for this IP and session."""
        if self.session_id:
            return f"{self.ip_address}:{self.session_id}"
        return self.ip_address


class LotteryQueueSelector:
    """
    Thread-safe registry and sorter for lottery queue numbers across multiple IPs.
    """

    def __init__(
        self,
        lower_is_better: bool = True,
        max_acceptable_position: Optional[int] = None,
    ) -> None:
        """
        Args:
            lower_is_better: True for waiting room queue ranks (1 is first in line),
                             False if higher number represents a higher raffle score.
            max_acceptable_position: Absolute cutoff position (e.g. 5000). Any ticket
                                     exceeding this number is considered non-viable.
        """
        self.lower_is_better = lower_is_better
        self.max_acceptable_position = max_acceptable_position
        self._tickets: Dict[str, LotteryTicket] = {}
        self._lock = threading.Lock()

    def register_ticket(
        self,
        ip_address: str,
        queue_number: int,
        session_id: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> LotteryTicket:
        """
        Registers or updates a lottery ticket received by an IP address.

        Args:
            ip_address: Source IP that received the queue number.
            queue_number: The assigned queue rank or lottery number.
            session_id: Optional session or cookie identifier.
            metadata: Additional platform-specific details.

        Returns:
            The created or updated LotteryTicket instance.
        """
        if not isinstance(queue_number, int):
            raise TypeError(f"queue_number must be an integer, got {type(queue_number).__name__}")

        if self.lower_is_better and queue_number < 1:
            raise ValueError(f"Queue rank must be positive (>= 1), got {queue_number}")

        ticket = LotteryTicket(
            ip_address=ip_address,
            queue_number=queue_number,
            session_id=session_id,
            status="waiting",
            metadata=metadata or {},
        )

        with self._lock:
            self._tickets[ticket.identifier] = ticket

        logger.info(
            "Registered lottery ticket for IP %s (session: %s): queue position #%d",
            ip_address,
            session_id or "default",
            queue_number,
        )
        return ticket

    def get_best_tickets(self, top_k: Optional[int] = None) -> List[LotteryTicket]:
        """
        Returns registered tickets sorted from best to worst.
        Filters out tickets worse than max_acceptable_position if configured.

        Args:
            top_k: Maximum number of winning tickets to return. None returns all sorted.
        """
        with self._lock:
            candidates = list(self._tickets.values())

        if self.max_acceptable_position is not None:
            if self.lower_is_better:
                candidates = [c for c in candidates if c.queue_number <= self.max_acceptable_position]
            else:
                candidates = [c for c in candidates if c.queue_number >= self.max_acceptable_position]

        candidates.sort(
            key=lambda t: t.queue_number,
            reverse=not self.lower_is_better,
        )

        if top_k is not None:
            return candidates[:top_k]
        return candidates

    def best_ticket(self) -> Optional[LotteryTicket]:
        """Returns the single best lottery ticket registered, or None if empty."""
        top_list = self.get_best_tickets(top_k=1)
        return top_list[0] if top_list else None

    def prune_non_viable(
        self,
        keep_top_k: int,
        max_position: Optional[int] = None,
    ) -> Tuple[List[LotteryTicket], List[LotteryTicket]]:
        """
        Separates tickets into kept winners and discarded losers.
        Updates internal statuses to 'selected' or 'discarded'.

        Args:
            keep_top_k: Number of best tickets to preserve.
            max_position: Override cutoff threshold if provided.

        Returns:
            Tuple of (kept_tickets, discarded_tickets).
        """
        cutoff = max_position if max_position is not None else self.max_acceptable_position

        with self._lock:
            all_tickets = list(self._tickets.values())

            # Sort best first
            all_tickets.sort(
                key=lambda t: t.queue_number,
                reverse=not self.lower_is_better,
            )

            kept: List[LotteryTicket] = []
            discarded: List[LotteryTicket] = []

            for ticket in all_tickets:
                is_within_cutoff = True
                if cutoff is not None:
                    if self.lower_is_better and ticket.queue_number > cutoff:
                        is_within_cutoff = False
                    elif not self.lower_is_better and ticket.queue_number < cutoff:
                        is_within_cutoff = False

                if len(kept) < keep_top_k and is_within_cutoff:
                    ticket.status = "selected"
                    kept.append(ticket)
                else:
                    ticket.status = "discarded"
                    discarded.append(ticket)

        return kept, discarded

    # [FEATURE: ADAPTIVE_LOTTERY_THRESHOLD] Golden threshold qualification and fallback retention
    # Raison: Preserves all sessions with exceptional queue numbers (e.g. <= 100) while guaranteeing minimum viable fallback
    # Attention: In massive drops, the statistical chance of all IPs being <= 100 is near zero; fallback retention prevents zero-selection lockout
    def all_under_threshold(self, threshold: int = 100) -> bool:
        """
        Returns True if all registered tickets have a queue number within the threshold.
        """
        with self._lock:
            if not self._tickets:
                return False
            if self.lower_is_better:
                return all(t.queue_number <= threshold for t in self._tickets.values())
            return all(t.queue_number >= threshold for t in self._tickets.values())

    def select_adaptive(
        self,
        golden_threshold: int = 100,
        min_keep: int = 1,
        max_keep: Optional[int] = None,
    ) -> Tuple[List[LotteryTicket], List[LotteryTicket]]:
        """
        Adaptive selection policy:
        1. If tickets meet the golden threshold (e.g. <= 100), keep all of them (up to max_keep).
        2. If all IPs are <= 100, all are preserved.
        3. If fewer than min_keep tickets meet the threshold, retain the top min_keep tickets
           as fallback to prevent losing all connections.

        Args:
            golden_threshold: Exceptional queue rank threshold (default: 100).
            min_keep: Minimum number of tickets to preserve even if none meet golden threshold.
            max_keep: Maximum tickets to preserve. None means no ceiling.

        Returns:
            Tuple of (kept_tickets, discarded_tickets).
        """
        with self._lock:
            all_tickets = list(self._tickets.values())

            # Sort best first
            all_tickets.sort(
                key=lambda t: t.queue_number,
                reverse=not self.lower_is_better,
            )

            qualifying: List[LotteryTicket] = []
            non_qualifying: List[LotteryTicket] = []

            for ticket in all_tickets:
                is_golden = (
                    ticket.queue_number <= golden_threshold
                    if self.lower_is_better
                    else ticket.queue_number >= golden_threshold
                )
                if is_golden:
                    qualifying.append(ticket)
                else:
                    non_qualifying.append(ticket)

            # Determine tickets to keep
            # Start with qualifying golden tickets
            kept = list(qualifying)

            # If fewer than min_keep, pad from best non-qualifying
            if len(kept) < min_keep:
                needed = min_keep - len(kept)
                kept.extend(non_qualifying[:needed])
                non_qualifying = non_qualifying[needed:]

            # Apply max_keep ceiling if specified
            if max_keep is not None and len(kept) > max_keep:
                overflow = kept[max_keep:]
                kept = kept[:max_keep]
                non_qualifying = overflow + non_qualifying

            # Re-sort non_qualifying into discarded
            discarded = non_qualifying

            # Update statuses
            for ticket in kept:
                ticket.status = "selected"
            for ticket in discarded:
                ticket.status = "discarded"

        return kept, discarded

    def clear(self) -> None:
        """Clears all registered tickets."""
        with self._lock:
            self._tickets.clear()

    def __bool__(self) -> bool:
        return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._tickets)


class MultiIpLotteryOrchestrator:
    """
    Coordinates concurrent lottery queue requests across multiple IP addresses
    and automatically aggregates the top-ranked winning tickets.
    """

    def __init__(
        self,
        selector: Optional[LotteryQueueSelector] = None,
        concurrency_limit: int = 10,
        timeout_sec: float = 10.0,
    ) -> None:
        self.selector = selector if selector is not None else LotteryQueueSelector()
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.timeout_sec = timeout_sec

    async def _survey_single_ip(
        self,
        ip_address: str,
        query_func: Callable[[str], Awaitable[int]],
    ) -> Optional[LotteryTicket]:
        """Queries a single IP with semaphore limiting and timeout protection."""
        async with self.semaphore:
            try:
                queue_number = await asyncio.wait_for(
                    query_func(ip_address),
                    timeout=self.timeout_sec,
                )
                return self.selector.register_ticket(
                    ip_address=ip_address,
                    queue_number=queue_number,
                )
            except Exception as query_error:
                logger.warning("Failed to obtain lottery number from IP %s: %s", ip_address, query_error)
                return None

    async def survey_pool(
        self,
        ips_or_pool: Union[SubnetIpPool, List[str]],
        query_func: Callable[[str], Awaitable[int]],
        top_k: int = 3,
        limit_ips: Optional[int] = None,
    ) -> List[LotteryTicket]:
        """
        Launches concurrent surveys across all IPs in the pool or list,
        then returns the top-K best lottery tickets.

        Args:
            ips_or_pool: SubnetIpPool instance or list of IP strings.
            query_func: Async callable taking an IP string and returning its queue number.
            top_k: Number of winning tickets to select.
            limit_ips: Optional limit on how many IPs from the pool to test.
        """
        if isinstance(ips_or_pool, SubnetIpPool):
            ip_list = ips_or_pool.get_hosts(limit=limit_ips or 256)
        else:
            ip_list = ips_or_pool if limit_ips is None else ips_or_pool[:limit_ips]

        tasks = [self._survey_single_ip(ip, query_func) for ip in ip_list]
        await asyncio.gather(*tasks, return_exceptions=True)

        return self.selector.get_best_tickets(top_k=top_k)
