#!/usr/bin/env python3
"""
Latency & Network Diagnostic Tool for Execution Algorithms
----------------------------------------------------------
Measures microsecond-level latency breakdown:
- DNS Resolution
- TCP 3-Way Handshake
- TLS 1.3/1.2 Negotiation
- Time-To-First-Byte (TTFB)
- Warm Reused Connection (Keep-Alive / HTTP/2)
- Statistical Jitter (p50, p95, min, max, stdev)

Can export results to JSON and compare Local vs Remote Server.
"""

import argparse
import asyncio
import json
import os
import platform
import socket
import ssl
import statistics
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


DEFAULT_TARGETS = [
    {
        "name": "Cloudflare Global Edge",
        "url": "https://cloudflare.com",
        "host": "cloudflare.com",
        "port": 443,
        "region": "Global Anycast",
    },
    {
        "name": "AWS EU Central (Frankfurt)",
        "url": "https://ec2.eu-central-1.amazonaws.com",
        "host": "ec2.eu-central-1.amazonaws.com",
        "port": 443,
        "region": "eu-central-1",
    },
    {
        "name": "AWS EU West (Paris)",
        "url": "https://ec2.eu-west-3.amazonaws.com",
        "host": "ec2.eu-west-3.amazonaws.com",
        "port": 443,
        "region": "eu-west-3",
    },
    {
        "name": "Google Edge",
        "url": "https://www.google.com",
        "host": "www.google.com",
        "port": 443,
        "region": "Global Anycast",
    },
    {
        "name": "Binance Public API",
        "url": "https://api.binance.com",
        "host": "api.binance.com",
        "port": 443,
        "region": "Financial Core (Tokyo/AWS)",
    },
]


def create_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def measure_single_cold_connection(host: str, port: int = 443, path: str = "/") -> Dict[str, Any]:
    """
    Measures DNS, TCP, TLS and TTFB sequentially using raw socket & ssl module.
    """
    metrics = {
        "dns_ms": 0.0,
        "tcp_ms": 0.0,
        "tls_ms": 0.0,
        "ttfb_ms": 0.0,
        "total_cold_ms": 0.0,
        "tls_version": "Unknown",
        "cipher": "Unknown",
        "ip": "Unknown",
        "success": False,
        "error": None,
    }

    t_start = time.perf_counter_ns()
    try:
        # 1. DNS Resolution
        t0 = time.perf_counter_ns()
        addr_info = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        t1 = time.perf_counter_ns()
        metrics["dns_ms"] = (t1 - t0) / 1_000_000.0
        ip_addr = addr_info[0][4][0]
        metrics["ip"] = ip_addr

        # 2. TCP 3-Way Handshake
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        t2 = time.perf_counter_ns()
        sock.connect((ip_addr, port))
        t3 = time.perf_counter_ns()
        metrics["tcp_ms"] = (t3 - t2) / 1_000_000.0

        # 3. TLS Handshake
        ssl_ctx = create_ssl_context()
        t4 = time.perf_counter_ns()
        secure_sock = ssl_ctx.wrap_socket(sock, server_hostname=host)
        t5 = time.perf_counter_ns()
        metrics["tls_ms"] = (t5 - t4) / 1_000_000.0
        metrics["tls_version"] = secure_sock.version() or "TLS"
        cipher_info = secure_sock.cipher()
        if cipher_info:
            metrics["cipher"] = cipher_info[0]

        # 4. HTTP Request & TTFB (Time To First Byte - Cold)
        http_req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: LatencyBench/1.0\r\n"
            f"Connection: keep-alive\r\n\r\n"
        ).encode("utf-8")

        t6 = time.perf_counter_ns()
        secure_sock.sendall(http_req)
        # Read the first byte
        first_byte = secure_sock.recv(1)
        t7 = time.perf_counter_ns()

        if first_byte:
            metrics["ttfb_ms"] = (t7 - t6) / 1_000_000.0
            metrics["success"] = True

            # 5. Measure pure Persistent Socket Keep-Alive (Immediate 2nd Request)
            try:
                # Read remainder of first response chunk quickly
                secure_sock.settimeout(0.3)
                _ = secure_sock.recv(4096)

                t8 = time.perf_counter_ns()
                secure_sock.sendall(http_req)
                second_first_byte = secure_sock.recv(1)
                t9 = time.perf_counter_ns()
                if second_first_byte:
                    metrics["warm_socket_ms"] = (t9 - t8) / 1_000_000.0
            except Exception:
                # Fallback to pure TTFB as minimum RTT indicator
                metrics["warm_socket_ms"] = metrics["ttfb_ms"]
        else:
            metrics["error"] = "Empty response"

        secure_sock.close()
    except Exception as e:
        metrics["error"] = str(e)
    finally:
        metrics["total_cold_ms"] = (
            metrics["dns_ms"] + metrics["tcp_ms"] + metrics["tls_ms"] + metrics["ttfb_ms"]
        )

    return metrics


async def measure_warm_keepalive(url: str, samples: int = 5) -> List[float]:
    """
    Measures warm requests over an already negotiated HTTP connection (Keep-Alive).
    """
    warm_latencies = []
    if not HTTPX_AVAILABLE:
        return warm_latencies

    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    # Check if HTTP/2 is supported by installed packages
    try:
        import h2  # noqa: F401
        has_http2 = True
    except ImportError:
        has_http2 = False

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "*/*",
    }
    async with httpx.AsyncClient(http2=has_http2, limits=limits, headers=headers, follow_redirects=True, verify=True, timeout=5.0) as client:
        # Pre-warm connection (Cold request)
        try:
            async with client.stream("GET", url) as _:
                pass
        except Exception:
            return warm_latencies

        # Measure repeated requests over the existing connection
        for _ in range(samples):
            t0 = time.perf_counter_ns()
            try:
                async with client.stream("GET", url) as res:
                    if res.status_code < 600:
                        t1 = time.perf_counter_ns()
                        warm_latencies.append((t1 - t0) / 1_000_000.0)
            except Exception:
                pass
            await asyncio.sleep(0.02)

    return warm_latencies


def compute_stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0, "p95": 0.0, "stdev": 0.0}
    vals = sorted(values)
    p95_idx = int(0.95 * len(vals))
    return {
        "min": round(min(vals), 2),
        "max": round(max(vals), 2),
        "mean": round(statistics.mean(vals), 2),
        "median": round(statistics.median(vals), 2),
        "p95": round(vals[min(p95_idx, len(vals) - 1)], 2),
        "stdev": round(statistics.stdev(vals) if len(vals) > 1 else 0.0, 2),
    }


async def run_benchmark(targets: List[Dict[str, Any]], samples: int = 5) -> Dict[str, Any]:
    system_info = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }

    results = []

    for target in targets:
        host = target["host"]
        port = target.get("port", 443)
        url = target.get("url", f"https://{host}")
        name = target.get("name", host)
        region = target.get("region", "N/A")

        cold_runs = []
        for _ in range(samples):
            res = measure_single_cold_connection(host, port)
            if res["success"]:
                cold_runs.append(res)
            time.sleep(0.05)

        warm_latencies = await measure_warm_keepalive(url, samples=samples)

        if cold_runs:
            dns_stats = compute_stats([r["dns_ms"] for r in cold_runs])
            tcp_stats = compute_stats([r["tcp_ms"] for r in cold_runs])
            tls_stats = compute_stats([r["tls_ms"] for r in cold_runs])
            ttfb_stats = compute_stats([r["ttfb_ms"] for r in cold_runs])
            total_stats = compute_stats([r["total_cold_ms"] for r in cold_runs])
            warm_socket_stats = compute_stats([r["warm_socket_ms"] for r in cold_runs if "warm_socket_ms" in r])
            warm_stats = warm_socket_stats if warm_socket_stats["median"] > 0 else compute_stats(warm_latencies)

            target_summary = {
                "name": name,
                "host": host,
                "ip": cold_runs[0]["ip"],
                "region": region,
                "tls_version": cold_runs[0]["tls_version"],
                "cipher": cold_runs[0]["cipher"],
                "dns": dns_stats,
                "tcp": tcp_stats,
                "tls": tls_stats,
                "ttfb": ttfb_stats,
                "total_cold": total_stats,
                "warm_keepalive": warm_stats,
                "samples_successful": len(cold_runs),
            }
        else:
            target_summary = {
                "name": name,
                "host": host,
                "error": "Failed all connection attempts",
                "samples_successful": 0,
            }

        results.append(target_summary)

    return {"system": system_info, "results": results}


def display_results_rich(data: Dict[str, Any]):
    sys_info = data["system"]
    console.print(
        Panel(
            f"[bold cyan]Système :[/bold cyan] {sys_info['platform']} | [bold cyan]Python :[/bold cyan] {sys_info['python_version']}\n"
            f"[bold cyan]Date (UTC) :[/bold cyan] {sys_info['timestamp']}",
            title="[bold green]Rapport de Diagnostic Réseau & Latence[/bold green]",
            expand=False,
        )
    )

    table = Table(title="Décomposition de la Latence Réseau (en millisecondes)")
    table.add_column("Cible", style="bold white", no_wrap=True)
    table.add_column("Région / IP", style="dim")
    table.add_column("DNS (p50)", justify="right")
    table.add_column("TCP Connect (p50)", justify="right")
    table.add_column("TLS Handshake (p50)", justify="right")
    table.add_column("TTFB (p50)", justify="right")
    table.add_column("Total Cold (p50)", justify="right", style="bold yellow")
    table.add_column("Warm Keep-Alive (p50)", justify="right", style="bold green")
    table.add_column("Gain Warm (%)", justify="right", style="bold magenta")

    for item in data["results"]:
        if item.get("samples_successful", 0) > 0:
            cold_p50 = item["total_cold"]["median"]
            warm_p50 = item["warm_keepalive"]["median"]
            if cold_p50 > 0 and warm_p50 > 0:
                gain = round(((cold_p50 - warm_p50) / cold_p50) * 100, 1)
                gain_str = f"+{gain}%"
            else:
                gain_str = "N/A"

            table.add_row(
                item["name"],
                f"{item.get('region', '')}\n[dim]{item.get('ip', '')}[/dim]",
                f"{item['dns']['median']} ms",
                f"{item['tcp']['median']} ms",
                f"{item['tls']['median']} ms",
                f"{item['ttfb']['median']} ms",
                f"{cold_p50} ms",
                f"{warm_p50} ms" if warm_p50 > 0 else "N/A",
                gain_str,
            )
        else:
            table.add_row(
                item["name"],
                item.get("host", ""),
                "[red]ERREUR[/red]",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
            )

    console.print(table)
    console.print(
        "\n[bold yellow]Explication concrète du Point 2 (Keep-Alive) :[/bold yellow]\n"
        "• [bold yellow]Total Cold[/bold yellow] = Le temps d'une requête classique qui réouvre tout (DNS + TCP + TLS + HTTP).\n"
        "• [bold]Warm Keep-Alive[/bold] = Le temps une fois la socket maintenue ouverte en mémoire (aucun handshake).\n"
        "• La colonne [bold magenta]Gain Warm[/bold magenta] montre l'accélération exacte obtenue sans toucher au code métier.\n"
    )


def compare_reports(file1: str, file2: str):
    """
    Compares two benchmark JSON reports (e.g. Local PC vs Remote Server).
    """
    with open(file1, "r", encoding="utf-8") as f:
        data1 = json.load(f)
    with open(file2, "r", encoding="utf-8") as f:
        data2 = json.load(f)

    sys1 = data1.get("system", {})
    sys2 = data2.get("system", {})

    if RICH_AVAILABLE:
        console.print(
            Panel(
                f"[bold cyan]Source A (Local / Machine 1) :[/bold cyan] {sys1.get('platform', 'Inconnu')}\n"
                f"[bold cyan]Source B (Serveur / Machine 2) :[/bold cyan] {sys2.get('platform', 'Inconnu')}",
                title="[bold green]Comparatif Latence : Machine A vs Machine B[/bold green]",
                expand=False,
            )
        )

        table = Table(title="Comparaison Face-à-Face (Médiane p50 en ms)")
        table.add_column("Cible", style="bold white")
        table.add_column("TCP A", justify="right")
        table.add_column("TCP B", justify="right")
        table.add_column("Warm A", justify="right")
        table.add_column("Warm B", justify="right")
        table.add_column("Vainqueur Warm", justify="center", style="bold")
        table.add_column("Différence", justify="right")

        dict2 = {item["host"]: item for item in data2.get("results", [])}

        for item1 in data1.get("results", []):
            host = item1["host"]
            item2 = dict2.get(host)
            if not item2 or item1.get("samples_successful", 0) == 0 or item2.get("samples_successful", 0) == 0:
                continue

            tcp_a = item1["tcp"]["median"]
            tcp_b = item2["tcp"]["median"]
            warm_a = item1["warm_keepalive"]["median"]
            warm_b = item2["warm_keepalive"]["median"]

            if warm_a > 0 and warm_b > 0:
                diff = abs(warm_a - warm_b)
                if warm_b < warm_a:
                    winner = "[green]Serveur B[/green]"
                    diff_pct = f"+{round(((warm_a - warm_b) / warm_a) * 100, 1)}% plus rapide"
                elif warm_a < warm_b:
                    winner = "[cyan]Local A[/cyan]"
                    diff_pct = f"+{round(((warm_b - warm_a) / warm_b) * 100, 1)}% plus rapide"
                else:
                    winner = "Égalité"
                    diff_pct = "0%"
            else:
                winner = "N/A"
                diff_pct = "N/A"

            table.add_row(
                item1["name"],
                f"{tcp_a} ms",
                f"{tcp_b} ms",
                f"{warm_a} ms",
                f"{warm_b} ms",
                winner,
                diff_pct,
            )

        console.print(table)
    else:
        print(f"Comparatif: {file1} vs {file2}")


def main():
    parser = argparse.ArgumentParser(description="Latency benchmark & network profiler for execution algorithms")
    parser.add_argument("--samples", type=int, default=5, help="Number of measurement samples per target (default: 5)")
    parser.add_argument("--target", type=str, default=None, help="Custom URL or domain to test (e.g. https://api.example.com)")
    parser.add_argument("--output", type=str, default=None, help="Save report to JSON file path")
    parser.add_argument("--compare", nargs=2, metavar=("REPORT_A", "REPORT_B"), help="Compare two existing JSON reports")

    args = parser.parse_args()

    if args.compare:
        compare_reports(args.compare[0], args.compare[1])
        return

    targets = DEFAULT_TARGETS
    if args.target:
        parsed = urlparse(args.target if "://" in args.target else f"https://{args.target}")
        targets = [
            {
                "name": f"Custom: {parsed.hostname}",
                "url": args.target if "://" in args.target else f"https://{args.target}",
                "host": parsed.hostname or args.target,
                "port": parsed.port or (443 if parsed.scheme == "https" else 80),
                "region": "Custom",
            }
        ]

    data = asyncio.run(run_benchmark(targets, samples=args.samples))

    if RICH_AVAILABLE:
        display_results_rich(data)
    else:
        print(json.dumps(data, indent=2))

    # Auto-save or explicitly save
    output_path = args.output
    if not output_path:
        os.makedirs("benchmarks/reports", exist_ok=True)
        filename = f"latency_report_{platform.node()}_{int(time.time())}.json"
        output_path = os.path.join("benchmarks", "reports", filename)

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    if RICH_AVAILABLE:
        console.print(f"[dim]Rapport JSON sauvegardé : {output_path}[/dim]\n")


if __name__ == "__main__":
    main()
