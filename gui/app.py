"""
Execution Algo - Mini Control Panel (Lightweight Floating GUI)
--------------------------------------------------------------
Ultra-compact GUI designed with Zero-Performance-Loss Architecture:
- The UI runs purely in its own thread to handle buttons & status display.
- The Execution Engine runs in a dedicated background worker thread with:
  * Windows HIGH_PRIORITY_CLASS CPU scheduling.
  * 1ms kernel timer interrupts (timeBeginPeriod).
  * Its own independent asyncio event loop.
  * Pre-warmed persistent TLS sockets in memory.
- Inter-thread communication is 100% non-blocking via memory queues.
"""

import asyncio
import os
import queue
import sys
import threading
import time
import tkinter as tk
from tkinter import ttk

# Add repository root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.engine.base import Signal
from core.network.persistent_client import PrewarmedHttpClient
from core.rate_limiter.limiter import AdaptiveRateLimiter
from core.system import boost_process_performance, restore_process_performance
from core.telemetry.tracker import LatencyTracker
from modules.retail.checkout.state_machine import (
    CheckoutProfile,
    CheckoutState,
    FastCheckoutStateMachine,
)
from modules.retail.clock.ntp_sync import HighPrecisionScheduler, NtpClient


class EngineWorker:
    """
    Dedicated background worker running the asyncio loop.
    Decoupled from GUI rendering to preserve microsecond execution speed.
    """

    def __init__(self, ui_queue: queue.Queue):
        self.ui_queue = ui_queue
        self.loop: asyncio.AbstractEventLoop = None
        self.fsm: FastCheckoutStateMachine = None
        self.http_client: PrewarmedHttpClient = None
        self.ntp = NtpClient()
        self.scheduler = HighPrecisionScheduler(self.ntp)
        self.telemetry = LatencyTracker()
        self.is_armed = False

    def start_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run_coro(self, coro):
        """Dispatches coroutine into worker event loop."""
        if self.loop and self.loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, self.loop)

    async def arm_engine(self, base_url: str, endpoint: str):
        boost_process_performance()
        self.ui_queue.put(("log", f">> Pré-chauffe de la socket TLS vers {base_url}..."))

        # 1. NTP Synchronization
        sync_res = self.ntp.sync()
        offset_ms = sync_res.get("median_offset_ms", 0.0)
        self.ui_queue.put(("ntp_offset", f"{offset_ms:+.1f} ms"))
        self.ui_queue.put(("log", f"[NTP] Décalage d'horloge calculé : {offset_ms:+.2f} ms"))

        # 2. Pre-warmed Client
        self.http_client = PrewarmedHttpClient(
            base_url=base_url,
            rate_limiter=AdaptiveRateLimiter(base_rate=5.0, burst_capacity=10.0),
            telemetry=self.telemetry,
        )

        profile = CheckoutProfile(
            email="client@example.com",
            shipping_address={"country": "FR", "city": "Paris"},
        )

        self.fsm = FastCheckoutStateMachine(
            target_domain=base_url,
            http_client=self.http_client,
            profile=profile,
            telemetry=self.telemetry,
        )

        await self.fsm.initialize()
        self.is_armed = True
        self.ui_queue.put(("status", ("ARMÉ", "#00E676")))
        self.ui_queue.put(("log", "[OK] Socket TLS connectée & maintenue en mémoire."))

    async def trigger_execution(self, item_id: str, endpoint: str, method: str):
        if not self.is_armed or not self.fsm:
            self.ui_queue.put(("log", "[ERREUR] Le moteur n'est pas armé."))
            return

        self.ui_queue.put(("status", ("EN COURS", "#FFD600")))
        self.ui_queue.put(("log", f">> DÉCLENCHEMENT IMMÉDIAT pour l'article: {item_id}..."))

        signal = Signal(
            source="gui_trigger",
            target_id=item_id,
            action="BUY",
            payload={
                "item_id": item_id,
                "quantity": 1,
                "reserve_endpoint": endpoint,
                "reserve_method": method,
                "shipping_endpoint": endpoint,
                "shipping_method": method,
            },
            urgency=3,
        )

        result = await self.fsm.execute(signal)

        if result.success:
            self.ui_queue.put(("status", ("SUCCÈS", "#00E676")))
            self.ui_queue.put(
                ("log", f"[✔] SUCCÈS ! HTTP {result.status_code} | Latence: {result.latency_ms:.2f} ms")
            )
            self.ui_queue.put(("latency", f"{result.latency_ms:.1f} ms"))
        else:
            self.ui_queue.put(("status", ("ÉCHEC", "#FF1744")))
            self.ui_queue.put(
                ("log", f"[✖] ÉCHEC : {result.error} (HTTP {result.status_code})")
            )


class MiniGuiApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Sniper Execution Panel")
        self.root.geometry("460x520")
        self.root.minsize(440, 500)
        self.root.configure(bg="#121212")

        # Bring window immediately to the front of user desktop
        self.root.lift()
        self.root.attributes("-topmost", True)
        self.root.after(200, lambda: self.root.attributes("-topmost", False))

        self.ui_queue = queue.Queue()
        self.worker = EngineWorker(self.ui_queue)

        # Launch dedicated worker thread
        self.worker_thread = threading.Thread(target=self.worker.start_loop, daemon=True)
        self.worker_thread.start()

        self._build_ui()
        self.root.after(50, self._poll_queue)

    def _build_ui(self):
        style = ttk.Style()
        style.theme_use("clam")

        # Top Header Card
        header_frame = tk.Frame(self.root, bg="#1E1E1E", pady=10, padx=12)
        header_frame.pack(fill="x", padx=10, pady=(10, 5))

        title_label = tk.Label(
            header_frame,
            text="⚡ EXECUTION ENGINE MINI-PANEL",
            font=("Segoe UI", 11, "bold"),
            fg="#FFFFFF",
            bg="#1E1E1E",
        )
        title_label.pack(side="left")

        self.status_badge = tk.Label(
            header_frame,
            text="EN ATTENTE",
            font=("Segoe UI", 9, "bold"),
            fg="#B0BEC5",
            bg="#263238",
            padx=8,
            pady=2,
        )
        self.status_badge.pack(side="right")

        # Configuration Inputs Frame
        form_frame = tk.Frame(self.root, bg="#1E1E1E", padx=12, pady=10)
        form_frame.pack(fill="x", padx=10, pady=5)

        # Target URL
        tk.Label(form_frame, text="URL Cible (API / Boutique) :", font=("Segoe UI", 9), fg="#B0BEC5", bg="#1E1E1E").pack(anchor="w")
        self.entry_url = tk.Entry(form_frame, font=("Segoe UI", 9), bg="#2A2A2A", fg="#FFFFFF", insertbackground="white")
        self.entry_url.insert(0, "https://api.binance.com")
        self.entry_url.pack(fill="x", pady=(2, 8))

        # Endpoint & Method
        row_ep = tk.Frame(form_frame, bg="#1E1E1E")
        row_ep.pack(fill="x", pady=(0, 8))

        tk.Label(row_ep, text="Endpoint :", font=("Segoe UI", 9), fg="#B0BEC5", bg="#1E1E1E").pack(side="left")
        self.entry_ep = tk.Entry(row_ep, font=("Segoe UI", 9), bg="#2A2A2A", fg="#FFFFFF", insertbackground="white", width=22)
        self.entry_ep.insert(0, "/api/v3/time")
        self.entry_ep.pack(side="left", padx=(5, 10))

        tk.Label(row_ep, text="Méthode :", font=("Segoe UI", 9), fg="#B0BEC5", bg="#1E1E1E").pack(side="left")
        self.combo_method = ttk.Combobox(row_ep, values=["GET", "POST"], width=6, state="readonly")
        self.combo_method.set("GET")
        self.combo_method.pack(side="left", padx=5)

        # Item ID
        tk.Label(form_frame, text="ID Article / Variante (ou Nom du Billet) :", font=("Segoe UI", 9), fg="#B0BEC5", bg="#1E1E1E").pack(anchor="w")
        self.entry_item = tk.Entry(form_frame, font=("Segoe UI", 9), bg="#2A2A2A", fg="#FFFFFF", insertbackground="white")
        self.entry_item.insert(0, "TICKET-CAT-1-CONCERT")
        self.entry_item.pack(fill="x", pady=(2, 4))

        # Action Buttons
        btn_frame = tk.Frame(self.root, bg="#121212")
        btn_frame.pack(fill="x", padx=10, pady=6)

        self.btn_arm = tk.Button(
            btn_frame,
            text="🔌 1. Armer & Pré-chauffer TLS",
            font=("Segoe UI", 9, "bold"),
            bg="#2979FF",
            fg="#FFFFFF",
            activebackground="#1565C0",
            relief="flat",
            pady=6,
            command=self._on_arm,
        )
        self.btn_arm.pack(side="left", fill="x", expand=True, padx=(0, 4))

        self.btn_fire = tk.Button(
            btn_frame,
            text="🚀 2. DÉCLENCHER",
            font=("Segoe UI", 9, "bold"),
            bg="#00E676",
            fg="#000000",
            activebackground="#00B0FF",
            relief="flat",
            pady=6,
            command=self._on_fire,
        )
        self.btn_fire.pack(side="right", fill="x", expand=True, padx=(4, 0))

        # Metrics Bar
        metrics_frame = tk.Frame(self.root, bg="#1E1E1E", padx=12, pady=6)
        metrics_frame.pack(fill="x", padx=10, pady=4)

        tk.Label(metrics_frame, text="Décalage NTP :", font=("Segoe UI", 8), fg="#78909C", bg="#1E1E1E").pack(side="left")
        self.lbl_ntp = tk.Label(metrics_frame, text="N/A", font=("Segoe UI", 8, "bold"), fg="#FFD54F", bg="#1E1E1E")
        self.lbl_ntp.pack(side="left", padx=(4, 15))

        tk.Label(metrics_frame, text="Dernière Latence :", font=("Segoe UI", 8), fg="#78909C", bg="#1E1E1E").pack(side="left")
        self.lbl_latency = tk.Label(metrics_frame, text="N/A", font=("Segoe UI", 8, "bold"), fg="#69F0AE", bg="#1E1E1E")
        self.lbl_latency.pack(side="left", padx=4)

        # Console / Logs
        log_frame = tk.Frame(self.root, bg="#1E1E1E", padx=10, pady=8)
        log_frame.pack(fill="both", expand=True, padx=10, pady=(4, 10))

        tk.Label(log_frame, text="Console de Télémétrie en Direct :", font=("Segoe UI", 8, "bold"), fg="#90A4AE", bg="#1E1E1E").pack(anchor="w")
        self.text_log = tk.Text(log_frame, bg="#181818", fg="#ECEFF1", font=("Consolas", 8), relief="flat", height=8)
        self.text_log.pack(fill="both", expand=True, pady=(4, 0))

    def log(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        self.text_log.insert("end", f"[{timestamp}] {message}\n")
        self.text_log.see("end")

    def _on_arm(self):
        url = self.entry_url.get().strip()
        ep = self.entry_ep.get().strip()
        self.status_badge.configure(text="PRÉ-CHAUFFE...", fg="#FFD600", bg="#37474F")
        self.worker.run_coro(self.worker.arm_engine(url, ep))

    def _on_fire(self):
        item = self.entry_item.get().strip()
        ep = self.entry_ep.get().strip()
        method = self.combo_method.get().strip()
        self.worker.run_coro(self.worker.trigger_execution(item, ep, method))

    def _poll_queue(self):
        try:
            while True:
                msg_type, data = self.ui_queue.get_nowait()
                if msg_type == "log":
                    self.log(data)
                elif msg_type == "status":
                    text, color = data
                    self.status_badge.configure(text=text, fg=color)
                elif msg_type == "ntp_offset":
                    self.lbl_ntp.configure(text=data)
                elif msg_type == "latency":
                    self.lbl_latency.configure(text=data)
        except queue.Empty:
            pass
        finally:
            self.root.after(50, self._poll_queue)


def launch_gui():
    root = tk.Tk()
    app = MiniGuiApp(root)
    root.mainloop()
    restore_process_performance()


if __name__ == "__main__":
    launch_gui()
