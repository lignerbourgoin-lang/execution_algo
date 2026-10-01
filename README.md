# Execution Algo Infrastructure

Infrastructure d'algorithmes d'exécution haute performance, déterministe et modulaire en Python.

---

## 1. Architecture du Dépôt

```text
execution_algo/
├── benchmarks/                  # Diagnostics réseau et profilage multi-points
│   ├── bench_latency.py         # Décomposition microseconde (DNS, TCP, TLS, TTFB, Keep-Alive)
│   └── reports/                 # Rapports JSON comparatifs (Local vs Hetzner)
│
├── config/                      # Profils d'exécution et cibles
│   ├── retail_drop.json.example # Template d'exemple sans PII
│   └── targets.json             # Cibles réseau de référence
│
├── core/                        # Cœur technique asynchrone commun
│   ├── engine/                  # BaseStrategy, BaseExecutor, Pipeline Orchestrator borné
│   ├── network/                 # Client HTTP/2 pré-connecté (Keep-Alive), WebSocket (Sequence Gaps)
│   ├── rate_limiter/            # Token Bucket et limiteur adaptatif dynamique
│   ├── system.py                # Optimisation OS Windows (timeBeginPeriod 1ms, HIGH_PRIORITY_CLASS)
│   └── telemetry/               # Chronométrage microseconde et traçabilité d'exécution
│
├── gui/                         # Interface graphique légère et isolée
│   └── app.py                   # Mini-GUI Tkinter asynchrone sur thread dédié (0% d'impact moteur)
│
├── modules/                     # Modules verticaux d'exécution
│   ├── finance/                 # TWAP, VWAP, Arbitrage Spatial, Routeur HMAC-SHA256
│   ├── marketplace/             # Seconde main (Feed Monitor, Filtrage structuré, Acheteur)
│   ├── mobility/                # Sniping de créneaux & réservations rapides
│   └── retail/                  # Horloge atomique NTP RFC 5905, Scheduler ms, State Machine
│
├── tests/                       # Suite de tests unitaires automatisés (30 tests)
│   ├── test_core.py             # Limiteurs, WebSocket gaps, client HTTP/2, Orchestrateur
│   ├── test_finance.py          # TWAP, VWAP, Arbitrage, Signature HMAC
│   ├── test_marketplace.py      # Déduplication de flux, regex de filtrage, acheteur
│   ├── test_mobility.py         # Sniping de dates/centres et réservation
│   └── test_retail.py           # NTP async, scheduler milliseconde, FSM sans état
│
├── requirements.txt             # Dépendances verrouillées (httpx[http2], h2, websockets, rich)
└── run.py                       # Point d'entrée CLI pour l'exécution d'un profil
```

---

## 2. Installation & Pré-requis

Python 3.11+ recommandé avec support HTTP/2 natif.

```bash
# Création et activation de l'environnement virtuel
python -m venv .venv
# Sur Windows :
.venv\Scripts\activate
# Sur Linux / macOS :
source .venv/bin/activate

# Installation des dépendances
pip install -r requirements.txt
```

---

## 3. Lancer les Tests Unitaires

La suite de tests couvre l'ensemble des modules réseau, finance, marketplace, retail et mobilité :

```bash
python -m unittest discover tests
```

---

## 4. Modules Métiers & Algorithmes Réels

### A. Finance (`modules/finance/`)
- **TWAP (`twap.py`)** : Découpage intelligent d'ordres parents en tranches d'exécution (slices) avec jitter pseudo-aléatoire sur les volumes pour éviter l'impact de marché et l'empreinte algorithmique. Intègre un garde-fou de prix limite (collar) et le calcul du slippage (en bps) par rapport à l'Arrival Price.
- **VWAP (`vwap.py`)** : Distribution pondérée selon un profil de volume réel ou intraday typique (courbe en U). Plafond strict de taux de participation (ex. max 15% du volume de l'intervalle) pour éviter la distorsion des cours. Suivi en temps réel de l'outperformance (bps) vs VWAP de marché.
- **Arbitrage Spatial (`arbitrage.py`)** : Comparaison temps-réel de spreads inter-plateformes (Venue A Bid vs Venue B Ask), déduction exacte des frais de transaction (taker fees) et buffer de slippage. Rejet automatique des cotations périmées (stale quotes) protégeant contre les faux signaux de flux WebSocket en retard.
- **Routeur d'Ordres (`order_router.py`)** : Exécution d'ordres financiers avec signature HMAC-SHA256, mapping strict de `client_order_id` (UUIDv4) garantissant une idempotence totale sans duplication.

### B. Retail & Billetterie (`modules/retail/`)
- **Synchronisation NTP RFC 5905 (`clock/ntp_sync.py`)** : Détection non-bloquante de l'offset de l'horloge système via un ensemble de serveurs de référence (Cloudflare, Google, pool.ntp.org), élimination des anomalies de routage (RTT > 1.5s).
- **Scheduler Milliseconde** : Planification avec réveil coopératif et verrouillage Windows `timeBeginPeriod(1)` pour réduire le jitter du scheduler OS de 15.6 ms à 1-2 ms.
- **State Machine sans état (`checkout/state_machine.py`)** : Chaque réservation tourne dans son propre `ExecutionContext` isolé, sans écrasement mutuel en exécution concurrente. Élimination complète de faux tokens de simulation : validation stricte du statut HTTP et échec explicite en cas d'anomalie.

### C. Seconde Main & Marketplace (`modules/marketplace/`)
- **Feed Monitor (`feed_monitor.py`)** : Déduplication en mémoire à capacité bornée (LRU) et calcul de la latence de découverte dès la publication d'une annonce.
- **Moteur de Filtrage (`filter_engine.py`)** : Évaluation regex pré-compilée ultra-rapide des titres et descriptions (mots-clés requis, liste noire d'exclusions type "pour pièces / cassé / fake"), fourchette de prix stricte, seuils de réputation vendeur (note mini, volume d'avis mini).
- **Acheteur Rapide (`buyer_executor.py`)** : Soumission d'achat/offre avec clé d'idempotence UUIDv4 sur socket HTTP/2 pré-connectée.

### D. Mobilité & Sniping de Créneaux (`modules/mobility/`)
- **Slot Sniper (`slot_sniper.py`)** : Détection et réservation instantanée de créneaux libérés (examens, rendez-vous administratifs, billets) selon une plage de dates et centres autorisés.

---

## 5. Mini-GUI d'Exécution

Pour lancer le panneau de contrôle visuel léger (thread-isolé, n'impactant pas la boucle d'événements réseau) :

```bash
python gui/app.py
```

Fonctionnalités :
- Saisie de l'URL cible et de l'Item ID.
- Configuration du délai d'avance (Lead Time ms).
- Pre-warm du socket TCP/TLS en 1 clic.
- Console de logs temps-réel.
- Lancement instantané ou programmé à la milliseconde.

---

## 6. Lancement en Ligne de Commande (CLI)

```bash
# Exemple avec configuration personnalisée
python run.py --config config/mon_drop.json

# Exemple en surcharge directe
python run.py --url https://mon-site.com --item BILLETS_FINALE_2026
```
