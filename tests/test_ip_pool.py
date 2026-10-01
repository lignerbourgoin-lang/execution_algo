"""
Unit tests for SubnetIpPool in core.network.ip_pool
"""

import os
import sys
import unittest

# Ensure execution_algo root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import httpx
from core.network.ip_pool import SubnetIpPool


class TestSubnetIpPool(unittest.TestCase):
    def test_ipv4_subnet_hosts(self):
        pool = SubnetIpPool("192.168.1.0/24")
        self.assertEqual(pool.version, 4)
        self.assertEqual(pool.cidr, "192.168.1.0/24")
        self.assertEqual(pool.network_address, "192.168.1.0")
        self.assertEqual(pool.broadcast_address, "192.168.1.255")
        self.assertEqual(pool.total_addresses, 256)
        self.assertEqual(pool.usable_host_count, 254)

        # Usable hosts start at .1 and end at .254
        self.assertEqual(pool.get_host_by_index(0), "192.168.1.1")
        self.assertEqual(pool.get_host_by_index(253), "192.168.1.254")

        # Out-of-bounds index must fail
        with self.assertRaises(IndexError):
            pool.get_host_by_index(254)
        with self.assertRaises(IndexError):
            pool.get_host_by_index(-1)

    def test_ipv4_point_to_point_and_single_host(self):
        # /31 point-to-point (RFC 3021)
        pool_31 = SubnetIpPool("10.0.0.0/31")
        self.assertEqual(pool_31.usable_host_count, 2)
        self.assertEqual(pool_31.get_host_by_index(0), "10.0.0.0")
        self.assertEqual(pool_31.get_host_by_index(1), "10.0.0.1")

        # /32 single host
        pool_32 = SubnetIpPool("10.0.0.5/32")
        self.assertEqual(pool_32.usable_host_count, 1)
        self.assertEqual(pool_32.get_host_by_index(0), "10.0.0.5")

    def test_ipv6_subnet(self):
        pool_v6 = SubnetIpPool("2001:db8::/120")
        self.assertEqual(pool_v6.version, 6)
        self.assertEqual(pool_v6.usable_host_count, 256)
        self.assertEqual(pool_v6.broadcast_address, "2001:db8::ff")
        self.assertEqual(pool_v6.get_host_by_index(0), "2001:db8::")
        self.assertEqual(pool_v6.get_host_by_index(255), "2001:db8::ff")

    def test_large_subnet_o1_lookup(self):
        # Subnet with 16 million addresses: must not allocate list in memory
        pool_large = SubnetIpPool("10.0.0.0/8")
        self.assertEqual(pool_large.usable_host_count, 16777214)
        self.assertEqual(pool_large.get_host_by_index(0), "10.0.0.1")
        self.assertEqual(pool_large.get_host_by_index(1000), "10.0.3.233")

    def test_round_robin_rotation(self):
        # /30 has 2 usable hosts: .1 and .2
        pool = SubnetIpPool("192.168.1.0/30")
        self.assertEqual(pool.usable_host_count, 2)

        self.assertEqual(pool.next_ip(), "192.168.1.1")
        self.assertEqual(pool.next_ip(), "192.168.1.2")
        # Wraps around
        self.assertEqual(pool.next_ip(), "192.168.1.1")
        self.assertEqual(pool.next_ip(), "192.168.1.2")

    def test_random_ip(self):
        pool = SubnetIpPool("172.16.0.0/24")
        usable_set = set(pool.get_hosts(limit=256))
        for _ in range(20):
            sampled_ip = pool.random_ip()
            self.assertIn(sampled_ip, usable_set)

    def test_iter_hosts_limit(self):
        pool = SubnetIpPool("192.168.1.0/24")
        hosts = list(pool.iter_hosts(limit=5))
        self.assertEqual(len(hosts), 5)
        self.assertEqual(hosts, [
            "192.168.1.1",
            "192.168.1.2",
            "192.168.1.3",
            "192.168.1.4",
            "192.168.1.5",
        ])

    def test_invalid_cidr_fail_closed(self):
        with self.assertRaises(ValueError):
            SubnetIpPool("invalid-ip/99")
        with self.assertRaises(ValueError):
            SubnetIpPool("300.300.300.300/24")

    def test_validate_local_binding(self):
        # 127.0.0.1 is always locally available on standard loopback
        self.assertTrue(SubnetIpPool.validate_local_binding("127.0.0.1"))

        # TEST-NET-3 RFC 5737 (203.0.113.0/24) is not provisioned locally
        self.assertFalse(SubnetIpPool.validate_local_binding("203.0.113.1"))

    def test_create_transport_binding(self):
        pool = SubnetIpPool("127.0.0.0/24")

        # Binding to 127.0.0.1 with verification succeeds
        transport = pool.create_transport(local_address="127.0.0.1", verify_binding=True)
        self.assertIsInstance(transport, httpx.AsyncHTTPTransport)
        self.assertEqual(getattr(transport._pool, "_keepalive_expiry", None), 60.0)

        # Custom limits are respected
        custom_limits = httpx.Limits(keepalive_expiry=120.0, max_connections=100)
        custom_transport = pool.create_transport(local_address="127.0.0.1", limits=custom_limits)
        self.assertEqual(getattr(custom_transport._pool, "_keepalive_expiry", None), 120.0)

        # Binding to an unassigned IP with verify_binding=True raises OSError
        with self.assertRaises(OSError):
            pool.create_transport(local_address="203.0.113.42", verify_binding=True)


if __name__ == "__main__":
    unittest.main()
