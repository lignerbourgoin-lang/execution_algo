# Execution Algo Infrastructure

Infrastructure d'algorithmes d'exécution haute performance et modulaire.

## 1. Architecture du Dépôt

```text
execution_algo/
├── benchmarks/                  # Outils de diagnostic réseau et profiling
│   ├── bench_latency.py         # Benchmarkeur microseconde (DNS, TCP, TLS, TTFB, Warm Keep-Alive)
│   └── reports/                 # Rapports JSON de latence (local vs serveur)
│
├── core/                        # Cœur technique commun réutilisable
│   ├── network/                 # Clients optimisés HTTP/2/3, WebSocket, Connection Pooling
│   ├── engine/                  # Boucle d'événements, dispatcher de tâches, scheduler
│   ├── rate_limiter/            # Token Bucket, gestion adaptative de débit
│   └── telemetry/               # Logging structuré, métriques et chronométrage
│
├── modules/                     # Modules d'exécution par domaine métier
│   ├── finance/                 # Trading & Exécution d'ordres (TWAP, VWAP, Arbitrage)
│   ├── retail/                  # Drops & billetterie
│   ├── mobility/                # Dispatch & réactivité VTC / transport
│   └── marketplace/             # Seconde main & enchères
│
└── config/                      # Configurations environnement & stratégies
```

---

## 2. Phase 1 : Benchmark et Diagnostic Réseau

L'utilitaire `benchmarks/bench_latency.py` permet de mesurer précisément la décomposition de latence sur plusieurs cibles stratégiques (Cloudflare, AWS Francfort, AWS Paris, Google, Binance API).

### Exécuter le benchmark en local :
```bash
python benchmarks/bench_latency.py --samples 5
```

### Exécuter sur une cible personnalisée :
```bash
python benchmarks/bench_latency.py --target https://api.votre-cible.com --samples 5
```

### Exporter et comparer avec votre serveur distant :
1. Sur votre PC local :
   ```bash
   python benchmarks/bench_latency.py --output benchmarks/reports/local.json
   ```
2. Sur votre serveur distant :
   ```bash
   python benchmarks/bench_latency.py --output server.json
   ```
3. Rapatrier `server.json` et lancer la comparaison face-à-face :
   ```bash
   python benchmarks/bench_latency.py --compare benchmarks/reports/local.json server.json
   ```
