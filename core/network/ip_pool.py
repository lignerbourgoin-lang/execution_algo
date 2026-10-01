"""
Subnet IP Pool and Source Interface Binding
------------------------------------------
Manages IPv4/IPv6 CIDR subnets, sequential or round-robin host IP allocation,
and constructs local-bound network transports for multi-IP machines and proxies.
"""

from __future__ import annotations

import ipaddress
import random
import socket
import threading
from typing import Iterator, Optional, Union

import httpx


# [FEATURE: IP_SUBNET_POOL] Subnet IP pool management and local source binding
# Raison: Enables multi-IP rotation and CIDR subnet allocation for low-latency network executors
# Attention: Outgoing IP binding requires addresses to be provisioned on host OS interface
class SubnetIpPool:
    """
    High-performance, memory-efficient IP pool for IPv4 and IPv6 subnets.

    Provides O(1) indexed lookup, round-robin rotation, and factory methods
    for httpx client transports bound to specific source IP addresses.
    """

    def __init__(self, subnet_cidr: str, strict: bool = False) -> None:
        """
        Initializes an IP pool from a CIDR notation string.

        Args:
            subnet_cidr: CIDR string, e.g. '192.168.1.0/24' or '10.0.0.0/16'.
            strict: If True, host bits must be zero in subnet_cidr.
        """
        try:
            self._network: Union[ipaddress.IPv4Network, ipaddress.IPv6Network] = ipaddress.ip_network(
                subnet_cidr, strict=strict
            )
        except ValueError as parse_error:
            raise ValueError(f"Invalid CIDR notation '{subnet_cidr}': {parse_error}") from parse_error

        self._lock = threading.Lock()
        self._round_robin_counter = 0

    @property
    def cidr(self) -> str:
        """Returns normalized CIDR representation."""
        return str(self._network)

    @property
    def version(self) -> int:
        """Returns IP version (4 or 6)."""
        return self._network.version

    @property
    def total_addresses(self) -> int:
        """Returns total number of IP addresses in the subnet."""
        return self._network.num_addresses

    @property
    def usable_host_count(self) -> int:
        """
        Returns count of usable host addresses.
        For IPv4 <= /30, excludes network and broadcast addresses.
        For IPv4 /31, /32 and IPv6, returns total address count according to RFC specifications.
        """
        if self._network.version == 4:
            if self._network.prefixlen >= 31:
                return self._network.num_addresses
            return max(0, self._network.num_addresses - 2)
        return self._network.num_addresses

    @property
    def network_address(self) -> str:
        """Returns the base network address."""
        return str(self._network.network_address)

    @property
    def broadcast_address(self) -> Optional[str]:
        """Returns broadcast address for IPv4 subnets, or None for IPv6."""
        if hasattr(self._network, "broadcast_address"):
            return str(self._network.broadcast_address)
        return None

    def get_host_by_index(self, index: int) -> str:
        """
        Returns usable host IP at specified 0-based offset in O(1) time and memory.
        """
        total_usable = self.usable_host_count
        if total_usable == 0:
            raise IndexError("Subnet has no usable host addresses.")
        if index < 0 or index >= total_usable:
            raise IndexError(f"Host index {index} out of range (0 to {total_usable - 1}).")

        if self._network.version == 4 and self._network.prefixlen < 31:
            # First usable host is offset 1 (skipping network address)
            return str(self._network[index + 1])
        return str(self._network[index])

    def next_ip(self) -> str:
        """
        Thread-safe round-robin allocation of the next usable host IP.
        """
        total_usable = self.usable_host_count
        if total_usable == 0:
            raise RuntimeError("Cannot rotate IP: Subnet contains zero usable hosts.")

        with self._lock:
            current_index = self._round_robin_counter % total_usable
            self._round_robin_counter += 1

        return self.get_host_by_index(current_index)

    def random_ip(self) -> str:
        """
        Returns a randomly selected usable host IP in O(1) time.
        """
        total_usable = self.usable_host_count
        if total_usable == 0:
            raise RuntimeError("Cannot select random IP: Subnet contains zero usable hosts.")
        random_index = random.randint(0, total_usable - 1)
        return self.get_host_by_index(random_index)

    def iter_hosts(self, limit: Optional[int] = None) -> Iterator[str]:
        """
        Iterates over usable host IP addresses lazily without large memory buffers.
        """
        total_usable = self.usable_host_count
        max_items = total_usable if limit is None else min(limit, total_usable)
        for host_index in range(max_items):
            yield self.get_host_by_index(host_index)

    def get_hosts(self, limit: int = 256) -> list[str]:
        """
        Returns a bounded list of host IPs.
        Bounded by default to prevent accidental out-of-memory errors on large subnets.
        """
        return list(self.iter_hosts(limit=limit))

    @staticmethod
    def validate_local_binding(ip_address: str) -> bool:
        """
        Verifies whether an IP address is assigned to an active interface on the local operating system.
        Performs a test socket bind to test if the address is usable as source.
        """
        try:
            parsed_ip = ipaddress.ip_address(ip_address)
            socket_family = socket.AF_INET if parsed_ip.version == 4 else socket.AF_INET6
            with socket.socket(socket_family, socket.SOCK_DGRAM) as test_socket:
                test_socket.bind((ip_address, 0))
                return True
        except (OSError, ValueError):
            return False

    # [FEATURE: KEEP_ALIVE_POOL_LIMITS] Set transport limits to prevent premature keepalive connection closures
    # Raison: httpx.AsyncHTTPTransport defaults to keepalive_expiry=5.0s, closing sockets between 20s heartbeats.
    # Attention: limits passed to AsyncClient are ignored when custom transport is supplied.
    DEFAULT_KEEPALIVE_EXPIRY_SEC = 60.0
    DEFAULT_MAX_KEEPALIVE_CONNECTIONS = 20
    DEFAULT_MAX_CONNECTIONS = 50

    def create_transport(
        self,
        local_address: Optional[str] = None,
        http2: bool = True,
        verify_binding: bool = False,
        limits: Optional[httpx.Limits] = None,
    ) -> httpx.AsyncHTTPTransport:
        """
        Creates an httpx.AsyncHTTPTransport configured with the specified local source IP address and connection limits.

        Args:
            local_address: Source IP to bind to. If None, next_ip() is used.
            http2: Whether HTTP/2 is enabled on the transport.
            verify_binding: If True, validates local assignment before transport creation.
            limits: Optional custom httpx.Limits. If omitted, uses 60s keepalive and 20 max keepalive connections.

        Raises:
            OSError: If verify_binding is True and IP is not provisioned on local OS interface.
        """
        target_ip = local_address or self.next_ip()

        if verify_binding and not self.validate_local_binding(target_ip):
            raise OSError(
                f"Local IP binding validation failed: Address '{target_ip}' is not assigned "
                "to any local network interface on this machine."
            )

        effective_limits = limits or httpx.Limits(
            max_keepalive_connections=self.DEFAULT_MAX_KEEPALIVE_CONNECTIONS,
            max_connections=self.DEFAULT_MAX_CONNECTIONS,
            keepalive_expiry=self.DEFAULT_KEEPALIVE_EXPIRY_SEC,
        )

        return httpx.AsyncHTTPTransport(
            local_address=target_ip,
            http2=http2,
            limits=effective_limits,
        )
