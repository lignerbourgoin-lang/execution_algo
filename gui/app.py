"""
Billetterie & Drops - Sniper Execution Panel (Lightweight Floating GUI)
-----------------------------------------------------------------------
Panneau de contrôle ultra-léger et réactif spécialement dédié aux Billetteries et Drops:
- Zero-Performance-Loss Architecture : L'UI tourne sur son propre thread,
  tandis que le moteur d'exécution tourne sur un thread worker en haute priorité
  (HIGH_PRIORITY_CLASS, timer kernel 1ms, loop asyncio indépendante).
- Fonctionnalités billetterie :
  * Pré-chauffe de socket HTTP/2 (Keep-Alive) sur l'hôte billetterie
  * Synchronisation d'horloge atomique NTP (RFC 5905)
  * Déclenchement au millième de seconde (Drop Programmé ou Immédiat)
  * Mode Rattrapage de Paniers Expirés (Cart Release Sniping)
  * Bouton direct pour ouvrir le panier réservé dans le navigateur pour le paiement / 3D-Secure
"""

import asyncio
from datetime import datetime, timezone
import os
import queue
import sys
import threading
import time
import tkinter as tk
from tkinter import ttk
import webbrowser

# Add repository root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.network.persistent_client import PrewarmedHttpClient
from core.rate_limiter.limiter import AdaptiveRateLimiter
from core.system import boost_process_performance, restore_process_performance
from core.telemetry.tracker import LatencyTracker
from modules.retail.clock.ntp_sync import HighPrecisionScheduler, NtpClient
from modules.retail.tickets.ticket_engine import TicketConfig, TicketDropExecutor


class TicketWorker:
    """
    Worker asynchrone tournant en arrière-plan avec CPU boost.
    """

    def __init__(self, ui_queue: queue.Queue):
        self.ui_queue = ui_queue
        self.loop: asyncio.AbstractEventLoop = None
        self.http_client: PrewarmedHttpClient = None
        self.ntp = NtpClient()
        self.telemetry = LatencyTracker()
        self.executor: TicketDropExecutor = None
        self.is_armed = False
        self.checkout_url: str = ""

    def start_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run_coro(self, coro):
        if self.loop and self.loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, self.loop)

    async def arm_engine(
        self,
        target_url: str,
        event_id: str,
        category_id: str,
        quantity: int,
        lead_time_ms: float,
        drop_time_utc: float | None = None,
        auth_token: str = "",
        session_cookie: str = "",
    ):
        boost_process_performance()
        self.ui_queue.put(("log", f">> [1/3] Connexion & Pré-chauffe TLS vers {target_url}..."))

        # 1. Synchronisation NTP
        sync_res = await self.ntp.sync_async()
        offset_ms = sync_res.get("median_offset_ms", 0.0)
        self.ui_queue.put(("ntp_offset", f"{offset_ms:+.1f} ms"))
        self.ui_queue.put(("log", f"[NTP] Horloge atomique synchronisée : décalage {offset_ms:+.2f} ms"))

        # 2. Client HTTP persistant
        self.http_client = PrewarmedHttpClient(
            base_url=target_url,
            rate_limiter=AdaptiveRateLimiter(base_rate=5.0, burst_capacity=10.0),
            telemetry=self.telemetry,
        )

        cookies = {"session_id": session_cookie} if session_cookie else None

        config = TicketConfig(
            platform_name="billetterie",
            target_url=target_url,
            event_id=event_id,
            category_id=category_id,
            quantity=quantity,
            drop_time_utc=drop_time_utc,
            lead_time_ms=lead_time_ms,
            auth_token=auth_token if auth_token else None,
            session_cookies=cookies,
            auto_open_browser=True,
        )

        self.executor = TicketDropExecutor(
            config=config,
            http_client=self.http_client,
            ntp_client=self.ntp,
            telemetry=self.telemetry,
        )

        await self.executor.initialize()
        self.is_armed = True
        self.ui_queue.put(("status", ("ARMÉ & PRÊT", "#00E676")))
        self.ui_queue.put(("log", "[OK] Socket TLS connectée & session prête pour l'injection."))

    async def trigger_drop(self):
        if not self.is_armed or not self.executor:
            self.ui_queue.put(("log", "[ERREUR] Le moteur n'a pas été pré-chauffé. Cliquez d'abord sur Armer."))
            return

        self.ui_queue.put(("status", ("TIR EN COURS", "#FFD600")))
        self.ui_queue.put(("log", f">> RÉSERVATION IMMÉDIATE : {self.executor.config.quantity} place(s) [{self.executor.config.category_id}]..."))

        result = await self.executor.execute_drop()

        if result.success:
            cart = result.data.get("cart", {})
            self.checkout_url = cart.get("checkout_url", "")
            self.ui_queue.put(("status", ("PANIER OBTENU !", "#00E676")))
            self.ui_queue.put(("cart_url", self.checkout_url))
            self.ui_queue.put(
                ("log", f"[✔] PLACES RÉSERVÉES ! Panier : {cart.get('token')} | Latence: {result.latency_ms:.2f} ms")
            )
            self.ui_queue.put(("log", f"[!] Vous avez {cart.get('expires_in_sec', 600) / 60:.0f} min pour finaliser le paiement."))
            self.ui_queue.put(("latency", f"{result.latency_ms:.1f} ms"))
        else:
            self.ui_queue.put(("status", ("ÉCHEC DU DROP", "#FF1744")))
            self.ui_queue.put(("log", f"[✖] Échec de la réservation : {result.error} (HTTP {result.status_code})"))

    async def run_cart_release_sniper(self):
        if not self.is_armed or not self.executor:
            self.ui_queue.put(("log", "[ERREUR] Armez d'abord la billetterie."))
            return

        self.ui_queue.put(("status", ("SURVEILLANCE PANIERS", "#29B6F6")))
        self.ui_queue.put(("log", ">> Début de surveillance des paniers expirés (intervalle 500ms)..."))

        result = await self.executor.monitor_cart_releases(poll_interval_sec=0.5, max_duration_sec=600.0)
        if result and result.success:
            cart = result.data.get("cart", {})
            self.checkout_url = cart.get("checkout_url", "")
            self.ui_queue.put(("status", ("PANIER RATTRAPÉ !", "#00E676")))
            self.ui_queue.put(("cart_url", self.checkout_url))
            self.ui_queue.put(("log", f"[✔] PLACE RATTRAPÉE DANS UN PANIER EXPIRÉ !"))
        else:
            self.ui_queue.put(("status", ("AUCUN PANIER", "#B0BEC5")))
            self.ui_queue.put(("log", "[INFO] Fin de la fenêtre de surveillance des paniers."))


class BilletterieSniperApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("🎟️ Billetterie Sniper - Execution Engine")
        self.root.geometry("490x590")
        self.root.minsize(460, 560)
        self.root.configure(bg="#121212")

        # Topmost on launch
        self.root.lift()
        self.root.attributes("-topmost", True)
        self.root.after(200, lambda: self.root.attributes("-topmost", False))

        self.ui_queue = queue.Queue()
        self.worker = TicketWorker(self.ui_queue)
        self.checkout_url = ""

        # Background worker thread
        self.worker_thread = threading.Thread(target=self.worker.start_loop, daemon=True)
        self.worker_thread.start()

        self._build_ui()
        self.root.after(50, self._poll_queue)

    def _build_ui(self):
        style = ttk.Style()
        style.theme_use("clam")

        # Top Header Card
        header_frame = tk.Frame(self.root, bg="#1E1E1E", pady=8, padx=12)
        header_frame.pack(fill="x", padx=10, pady=(10, 5))

        title_label = tk.Label(
            header_frame,
            text="🎟️ BILLETTERIE & TICKETS SNIPER",
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

        # Form Inputs Frame
        form_frame = tk.Frame(self.root, bg="#1E1E1E", padx=12, pady=10)
        form_frame.pack(fill="x", padx=10, pady=5)

        # URL Billetterie
        tk.Label(form_frame, text="URL Billetterie / Hôte API :", font=("Segoe UI", 8, "bold"), fg="#90CAF9", bg="#1E1E1E").pack(anchor="w")
        self.entry_url = tk.Entry(form_frame, font=("Segoe UI", 9), bg="#2A2A2A", fg="#FFFFFF", insertbackground="white")
        self.entry_url.insert(0, "https://billetterie.example.com")
        self.entry_url.pack(fill="x", pady=(2, 6))

        # ID Événement & Catégorie
        row_event = tk.Frame(form_frame, bg="#1E1E1E")
        row_event.pack(fill="x", pady=(0, 6))

        frame_ev = tk.Frame(row_event, bg="#1E1E1E")
        frame_ev.pack(side="left", fill="x", expand=True, padx=(0, 5))
        tk.Label(frame_ev, text="ID Événement :", font=("Segoe UI", 8), fg="#B0BEC5", bg="#1E1E1E").pack(anchor="w")
        self.entry_event_id = tk.Entry(frame_ev, font=("Segoe UI", 9), bg="#2A2A2A", fg="#FFFFFF", insertbackground="white")
        self.entry_event_id.insert(0, "CONCERT-2026")
        self.entry_event_id.pack(fill="x", pady=2)

        frame_cat = tk.Frame(row_event, bg="#1E1E1E")
        frame_cat.pack(side="left", fill="x", expand=True, padx=(5, 0))
        tk.Label(frame_cat, text="Catégorie (Fosse / Cat 1...) :", font=("Segoe UI", 8), fg="#B0BEC5", bg="#1E1E1E").pack(anchor="w")
        self.entry_cat = tk.Entry(frame_cat, font=("Segoe UI", 9), bg="#2A2A2A", fg="#FFFFFF", insertbackground="white")
        self.entry_cat.insert(0, "CARRE_OR")
        self.entry_cat.pack(fill="x", pady=2)

        # Quantité & Lead Time (ms)
        row_qty_lead = tk.Frame(form_frame, bg="#1E1E1E")
        row_qty_lead.pack(fill="x", pady=(0, 6))

        frame_qty = tk.Frame(row_qty_lead, bg="#1E1E1E")
        frame_qty.pack(side="left", fill="x", expand=True, padx=(0, 5))
        tk.Label(frame_qty, text="Nombre de Billets :", font=("Segoe UI", 8), fg="#B0BEC5", bg="#1E1E1E").pack(anchor="w")
        self.combo_qty = ttk.Combobox(frame_qty, values=["1", "2", "3", "4"], width=6, state="readonly")
        self.combo_qty.set("2")
        self.combo_qty.pack(anchor="w", pady=2)

        frame_lead = tk.Frame(row_qty_lead, bg="#1E1E1E")
        frame_lead.pack(side="left", fill="x", expand=True, padx=(5, 0))
        tk.Label(frame_lead, text="Avance Firing (Lead Time ms) :", font=("Segoe UI", 8), fg="#B0BEC5", bg="#1E1E1E").pack(anchor="w")
        self.entry_lead = tk.Entry(frame_lead, font=("Segoe UI", 9), bg="#2A2A2A", fg="#FFFFFF", insertbackground="white", width=10)
        self.entry_lead.insert(0, "35.0")
        self.entry_lead.pack(anchor="w", pady=2)

        # Session Auth / Cookie (Optionnel)
        tk.Label(form_frame, text="Session Cookie / Auth Token (Optionnel si compte connecté) :", font=("Segoe UI", 8), fg="#B0BEC5", bg="#1E1E1E").pack(anchor="w")
        self.entry_cookie = tk.Entry(form_frame, font=("Segoe UI", 8), bg="#2A2A2A", fg="#FFFFFF", insertbackground="white")
        self.entry_cookie.insert(0, "")
        self.entry_cookie.pack(fill="x", pady=(2, 2))

        # Action Buttons Grid
        btn_frame = tk.Frame(self.root, bg="#121212")
        btn_frame.pack(fill="x", padx=10, pady=4)

        row_btn1 = tk.Frame(btn_frame, bg="#121212")
        row_btn1.pack(fill="x", pady=2)

        self.btn_arm = tk.Button(
            row_btn1,
            text="🔌 1. Pré-chauffer TLS & Horloge",
            font=("Segoe UI", 9, "bold"),
            bg="#1976D2",
            fg="#FFFFFF",
            activebackground="#1565C0",
            relief="flat",
            pady=6,
            command=self._on_arm,
        )
        self.btn_arm.pack(side="left", fill="x", expand=True, padx=(0, 4))

        self.btn_fire = tk.Button(
            row_btn1,
            text="🚀 2. DÉCLENCHER RÉSERVATION",
            font=("Segoe UI", 9, "bold"),
            bg="#00E676",
            fg="#000000",
            activebackground="#00C853",
            relief="flat",
            pady=6,
            command=self._on_fire,
        )
        self.btn_fire.pack(side="right", fill="x", expand=True, padx=(4, 0))

        row_btn2 = tk.Frame(btn_frame, bg="#121212")
        row_btn2.pack(fill="x", pady=4)

        self.btn_release = tk.Button(
            row_btn2,
            text="🔄 3. Rattrapage Paniers Expirés",
            font=("Segoe UI", 9),
            bg="#37474F",
            fg="#ECEFF1",
            activebackground="#455A64",
            relief="flat",
            pady=5,
            command=self._on_release_snipe,
        )
        self.btn_release.pack(side="left", fill="x", expand=True, padx=(0, 4))

        self.btn_open_cart = tk.Button(
            row_btn2,
            text="💳 4. Ouvrir Panier (Paiement)",
            font=("Segoe UI", 9, "bold"),
            bg="#212121",
            fg="#757575",
            state="disabled",
            relief="flat",
            pady=5,
            command=self._on_open_cart,
        )
        self.btn_open_cart.pack(side="right", fill="x", expand=True, padx=(4, 0))

        # Metrics & NTP Bar
        metrics_frame = tk.Frame(self.root, bg="#1E1E1E", padx=12, pady=5)
        metrics_frame.pack(fill="x", padx=10, pady=3)

        tk.Label(metrics_frame, text="Offset Horloge NTP :", font=("Segoe UI", 8), fg="#78909C", bg="#1E1E1E").pack(side="left")
        self.lbl_ntp = tk.Label(metrics_frame, text="N/A", font=("Segoe UI", 8, "bold"), fg="#FFD54F", bg="#1E1E1E")
        self.lbl_ntp.pack(side="left", padx=(4, 15))

        tk.Label(metrics_frame, text="Latence Réseau :", font=("Segoe UI", 8), fg="#78909C", bg="#1E1E1E").pack(side="left")
        self.lbl_latency = tk.Label(metrics_frame, text="N/A", font=("Segoe UI", 8, "bold"), fg="#69F0AE", bg="#1E1E1E")
        self.lbl_latency.pack(side="left", padx=4)

        # Telemetry & Logs
        log_frame = tk.Frame(self.root, bg="#1E1E1E", padx=10, pady=6)
        log_frame.pack(fill="both", expand=True, padx=10, pady=(3, 10))

        tk.Label(log_frame, text="Console de Télémétrie en Direct :", font=("Segoe UI", 8, "bold"), fg="#90A4AE", bg="#1E1E1E").pack(anchor="w")
        self.text_log = tk.Text(log_frame, bg="#181818", fg="#ECEFF1", font=("Consolas", 8), relief="flat", height=7)
        self.text_log.pack(fill="both", expand=True, pady=(3, 0))

    def log(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        self.text_log.insert("end", f"[{timestamp}] {message}\n")
        self.text_log.see("end")

    def _on_arm(self):
        url = self.entry_url.get().strip()
        event_id = self.entry_event_id.get().strip()
        cat = self.entry_cat.get().strip()
        qty = int(self.combo_qty.get().strip() or "1")
        lead = float(self.entry_lead.get().strip() or "35.0")
        cookie = self.entry_cookie.get().strip()

        self.status_badge.configure(text="PRÉ-CHAUFFE...", fg="#FFD600", bg="#37474F")
        self.worker.run_coro(
            self.worker.arm_engine(
                target_url=url,
                event_id=event_id,
                category_id=cat,
                quantity=qty,
                lead_time_ms=lead,
                session_cookie=cookie,
            )
        )

    def _on_fire(self):
        self.worker.run_coro(self.worker.trigger_drop())

    def _on_release_snipe(self):
        self.worker.run_coro(self.worker.run_cart_release_sniper())

    def _on_open_cart(self):
        if self.checkout_url:
            webbrowser.open(self.checkout_url)

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
                elif msg_type == "cart_url":
                    self.checkout_url = data
                    if self.checkout_url:
                        self.btn_open_cart.configure(
                            state="normal",
                            bg="#00E676",
                            fg="#000000",
                        )
        except queue.Empty:
            pass
        finally:
            self.root.after(50, self._poll_queue)


def launch_gui():
    root = tk.Tk()
    app = BilletterieSniperApp(root)
    root.mainloop()
    restore_process_performance()


if __name__ == "__main__":
    launch_gui()
