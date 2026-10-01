"""
Tickets Module
--------------
Automated ticketing drops, clock synchronization, and cart release sniping:
- TicketConfig
- CartReservation
- TicketDropExecutor
"""

from modules.retail.tickets.lottery_selector import (
    LotteryQueueSelector,
    LotteryTicket,
    MultiIpLotteryOrchestrator,
)
from modules.retail.tickets.queue_worker import (
    AdmissionHandoff,
    HeadlessQueueWorker,
    QueueWorkerConfig,
    find_chrome_executable,
)
from modules.retail.tickets.ticket_engine import (
    CartReservation,
    TicketConfig,
    TicketDropExecutor,
)

__all__ = [
    "TicketConfig",
    "CartReservation",
    "TicketDropExecutor",
    "LotteryTicket",
    "LotteryQueueSelector",
    "MultiIpLotteryOrchestrator",
    "HeadlessQueueWorker",
    "QueueWorkerConfig",
    "AdmissionHandoff",
    "find_chrome_executable",
]
