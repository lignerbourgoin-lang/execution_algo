# Changelog Features

## [2026-10-02] AUTOPILOT_MULTI_IP_STAGGERED_AND_TELEMETRY_DASHBOARD
**Fichiers**: `gui/app.py`, `core/telemetry/tracker.py`, `core/network/persistent_client.py`, `tests/test_core.py`, `tests/test_tickets.py`
**Raison**: Intégration de bout en bout du tir échelonné multi-IP avec disjoncteur dans l'autopilote GUI (zéro latence de décision humaine), enrichissement du traceur de télémétrie avec distribution des codes HTTP et percentiles p90/p95/p99, et réarmement automatique sur incident réseau.
**Logique**:
- **Pipeline Autopilote Multi-IP Échelonné (`AUTOPILOT_MULTI_IP_STAGGERED`)** : Dans `TicketWorker.run_autopilot_pipeline`, conservation de l'ensemble des tickets d'or qualifiés (`kept_tickets`) issus du tirage tombola simultané sur 20 IPs. Pré-chauffe parallèle de toutes les sockets HTTP/2 candidates et instanciation du `StaggeredDropOrchestrator` avec `IpCircuitBreakerPool`. Si le palier 0 rencontre une défaillance ou un inventaire saturé, le palier suivant prend immédiatement le relais à +25ms. L'événement de victoire annule instantanément tous les tirs concurrents en vol.
- **Tableau de bord télémétrique et percentiles (`DASHBOARD_REPORTER`)** : Ajout dans `LatencyTracker` de percentiles fins (p50, p90, p95, p99), d'un comptage granulaire de la distribution des codes d'état HTTP, et de la méthode `generate_dashboard_report()` pour affichage direct opérateur ou console.
- **Réinitialisation de pré-chauffe sur erreur socket (`PERSISTENT_CLIENT_ERROR_RESET`)** : En cas de `httpx.HTTPError` dans `send_fast()`, passage de `is_warmed_up` à `False` et enregistrement du code HTTP 0 dans la trace pour forcer une réouverture propre sans tenter d'exploiter un canal rompu.
- **Couverture de tests unitaire intégrale** : Ajout de `test_dashboard_report_and_status_distribution` dans `test_core.py` et de `TestTicketWorkerAutopilot` dans `test_tickets.py`. Suite complète portée à 151 tests passants.
**Attention**:
- Le disjoncteur IP isole automatiquement les adresses throttlées (429) ou challengées (WAF) pour concentrer les tirs ultérieurs uniquement sur les sessions saines.

## [2026-10-01] HARDENING_AND_SUBMILLIS_OPTIMIZATION
**Fichiers**: `core/network/ip_pool.py`, `core/network/persistent_client.py`, `core/network/waf_detector.py`, `core/telemetry/tracker.py`, `modules/retail/clock/ntp_sync.py`, `modules/retail/tickets/staggered_executor.py`, `modules/retail/tickets/ticket_engine.py`, `run.py`, `tests/test_core.py`, `tests/test_ip_pool.py`, `tests/test_staggered_executor.py`, `tests/test_waf_detector.py`, `benchmarks/bench_t0_drop_20_ips.py`
**Raison**: Résolution des goulots d'étranglement de socket (fermeture intempestive à 5s), élimination du chevauchement de spin-wait multi-IP, décodage JSON direct sans allocation, calibration fine de l'horloge Windows NT, détection des salles d'attente virtuelles (Queue-it / Turnstile 200/302), et unification du CLI `run.py`.
**Logique**:
- **Persistance Keep-Alive sur transport IP (`KEEP_ALIVE_POOL_LIMITS`)** : Injection explicite de `httpx.Limits(keepalive_expiry=60.0)` dans `SubnetIpPool.create_transport`. Comble le défaut httpx qui fermait les sockets après 5s alors que le heartbeat était à 20s, évitant tout handshake TLS à froid au T0.
- **Coordination centralisée du tir échelonné (`STAGGERED_TIMING_SYNC`)** : Calcul exact de l'instant de tir par palier (`target_tier_epoch = base_drop_utc + tier_delay_sec`) et transmission de `skip_scheduling=True` aux exécuteurs. Évite l'écroulement des paliers sur T0 et supprime la sérialisation des boucles de spin entre adresses IP du palier 0.
- **Décodage direct sans allocation (`ZERO_ALLOC_DECODE`)** : Remplacement de l'accès préalable à `response.text` par `json.loads(response.content)`. Sur les erreurs HTTP (403, 503), troncature du corps à 256 octets pour éviter l'allocation en mémoire de pages HTML CDN volumineuses (50 Ko à 200 Ko).
- **Calibration du spin Windows NT (`WINDOWS_SPIN_TUNING`)** : Réduction de `COARSE_SLEEP_MARGIN_MS` de 100 ms à 15 ms (-85% de CPU gaspillé en sommeil coopératif), et élargissement de `HARD_SPIN_WINDOW_MS` de 1.0 ms à 3.5 ms pour absorber le jitter d'ordonnancement Proactor Windows.
- **Détection des files d'attente virtuelles (`WAF_WAITING_ROOM_DETECTION`)** : Inspection proactive des redirections HTTP 302/303 vers `queue-it.net` et des pages intercalaires Cloudflare Turnstile renvoyant HTTP 200 OK avec payload JavaScript.
- **Point d'entrée unifié et résilient (`UNIFIED_RUNNER_PIPELINE`)** : Support direct du mode billetterie (`--mode ticket`) et retail, parsing robuste des horodatages ISO 8601 et Unix float, sécurisation de la restauration de priorité process via `try...finally: restore_process_performance()`, et correction de la clé de précision temporelle `dispatch_error_us`.
- **Validation d'accord de niveau de service SLA (`PREFLIGHT_SLA_CHECK`)** : Contrôle automatique pré-tir à T-30s sur la latence réseau p95 et la dérive d'horloge atomique.
**Attention**:
- Les sockets locales liées via `SubnetIpPool` nécessitent que les adresses soient provisionnées sur l'interface hôte. En environnement distant, utiliser un pool de proxies résidentiels rotatifs.

## [2026-10-01] STAGGERED_DROP_AND_CIRCUIT_BREAKER
**Fichiers**: `core/network/circuit_breaker.py`, `core/network/waf_detector.py`, `core/telemetry/tracker.py`, `modules/retail/tickets/staggered_executor.py`, `tests/mock_drop_server.py`, `examples/demo_full_drop_simulation.py`
**Raison**: Securisation des tirs multi-IP au T0 par echelonnement temporel (stagger), isolation proactive des proxies defaillants ou defies par WAF (circuit breaker), et serveur mock de simulation haute-fidelite.
**Logique**:
- **Circuit Breaker par IP (`IpCircuitBreakerPool`)** : Suivi fin de l'etat de chaque proxy (HEALTHY, THROTTLED, CHALLENGED, BURNED, HALF_OPEN). Mise en quarantaine automatique sur HTTP 403, 429 ou timeouts repetes, protegeant le pool au T0.
- **Detecteur WAF passif et actif (`detect_waf_challenge`)** : Analyse fine des headers (cf-mitigated, cf-ray, x-datadome) et du body (Turnstile, Just a moment...) pour isoler les defis WAF et declencher un basculement navigateur si requis.
- **Orchestrateur de tir echelonne (`StaggeredDropOrchestrator`)** : Repartition des IP en paliers temporels (ex: T0, T0+25ms, T0+50ms). Des qu'un palier decroche un panier, un evenement d'arret annule instantanement tous les autres tirs en vol, eliminant les doublons et respectant les quotas.
- **Export d'audit nanoseconde (`export_audit_json`)** : Sauvegarde structuree de l'ensemble des metriques, breakdowns d'etapes et percentiles (p50, p95) pour analyse post-mortem.
- **Serveur Mock realiste (`MockTicketingServer`)** : Simulation complete en memoire avec ouverture a T0, epuisement d'inventaire (409), liberation automatique des paniers expires pour wave sniping, et injection de challenges Cloudflare.
**Attention**:
- Les paliers d'echelonnement (stagger_interval_ms) doivent etre calibres entre 20 et 40 ms selon le nombre d'IP pour couvrir la fenetre d'ouverture sans saturer les limites de requetes par seconde.

## [2026-10-01] ENGINE_PERF_OPTIMIZATIONS
**Fichiers**: `core/rate_limiter/limiter.py`, `core/network/persistent_client.py`, `modules/retail/tickets/ticket_engine.py`, `tests/test_core.py`, `tests/test_ticketing.py`, `tests/test_tickets.py`
**Raison**: Optimisation du chemin critique de tirage et de surveillance des paniers expires (wave sniping) pour eliminer les doubles reveils asyncio, la derive temporelle de polling et la surcharge systeme au T0.
**Logique**:
- **Rate limiter unifie** : Fusion du calcul de penalite (HTTP 429/503 Retry-After) et du deficit de jetons dans `TokenBucketLimiter.acquire(..., min_start_ns=...)`. Remplacement du double sommeil sequentiel par un unique `asyncio.sleep()`, garantissant un espacement parfait des requetes sans reveil parasite de la boucle d'evenements.
- **Polling par echeance stricte (Deadline-Based)** : Dans `monitor_cart_releases()`, substitution de l'ancien `sleep(interval + latence)` par un cadencement base sur horloge monotone (`cycle_deadline_monotonic = max(...)`). Les vagues de sniping (ex: 150 ms) conservent une cadence stricte et eliminent toute derive cumulative due aux allers-retours reseau.
- **Prechauffage sur route API ciblee** : Extension de `PrewarmedHttpClient` et `start()` avec `probe_path` parametrable. `TicketDropExecutor` prechauffe directement la route API de billetterie (`/api/events/{id}/availability`) plutot que la racine CDN statique `/`.
- **Preconstruction de burst sans allocation** : Generation en amont de `_prebuilt_burst_requests` pour la categorie principale avec cles d'idempotence distinctes. Au T0, les reessais ultra-rapides (micro-burst) reutilisent ces requetes preconstruites sans serialisation JSON ni appel systeme d'entropie UUID (`CryptGenRandom`).
- **Elagage architectural** : Suppression du dossier duplique obsolète `sniper/` et assainissement des caches `__pycache__`.
**Attention**:
- Chaque tentative du micro-burst dispose de sa propre cle d'idempotence precalculee unique pour eviter tout rejet par deduplication cote serveur distant.

## [2026-10-01] IP_SUBNET_POOL
**Fichiers**: `core/network/ip_pool.py`, `core/network/__init__.py`, `tests/test_ip_pool.py`
**Raison**: Gestion de pools d'adresses IP pour le découpage de sous-réseaux CIDR, la rotation de requêtes et le binding sur interfaces réseau locales.
**Logique**:
- Analyse et validation de masques CIDR IPv4 et IPv6 via `ipaddress.ip_network`.
- Indexation et accès aux adresses hôtes en complexité O(1) mémoire et temps (sans instanciation de listes volumineuses en RAM sur les gros blocs comme les /16 ou /8).
- Rotation d'adresses en Round-Robin avec verrou thread-safe et sélection aléatoire O(1).
- Détection et validation préventive de binding local OS (vérification socket UDP non-bloquante fail-closed).
- Usine de transport asynchrone `httpx.AsyncHTTPTransport(local_address=...)` pour intégration immédiate avec `PrewarmedHttpClient`.
**Attention**:
- Une adresse IP générée en mémoire ne peut être utilisée comme adresse source sortante sur Internet que si elle est préalablement provisionnée et routée sur les interfaces réseau de la machine hôte.
- Pour des adresses publiques distantes sans infrastructure multi-IP locale, utiliser un pool de proxies HTTP/SOCKS5.

## [2026-10-01] LOTTERY_QUEUE_SELECTOR
**Fichiers**: `modules/retail/tickets/lottery_selector.py`, `modules/retail/tickets/__init__.py`, `tests/test_lottery_selector.py`
**Raison**: Dans les files d'attente virtuelles (Queue-It, Ticketmaster, AXS), les numéros de passage ("tombola" ou queue rank) attribués aux différentes adresses IP sont aléatoires. L'algorithme conserve uniquement les meilleurs numéros (les plus petits) pour concentrer l'exécution sur les sessions admises.
**Logique**:
- Enregistrement thread-safe des tickets de tombola/file par IP et session via `LotteryQueueSelector`.
- Tri et sélection des Top-K meilleurs numéros (`lower_is_better=True` par défaut pour les positions de file).
- Filtrage par seuil de coupure (`max_acceptable_position`) pour éliminer immédiatement les positions sans espoir (ex: position 50 000 quand il y a 2 000 places).
- Élagage propre (`prune_non_viable`) séparant les tickets "selected" des "discarded" pour fermer les sockets et sessions inutiles.
- Orchestration asynchrone concurrente (`MultiIpLotteryOrchestrator`) avec sémaphore de concurrence et timeout par IP.
**Attention**:
- Un numéro de file bas ne garantit l'achat que si le token/cookie de session associé à cette IP est conservé pour la phase de checkout.

## [2026-10-01] ADAPTIVE_LOTTERY_THRESHOLD
**Fichiers**: `modules/retail/tickets/lottery_selector.py`, `tests/test_lottery_selector.py`
**Raison**: Permettre la rétention dynamique de tous les tickets d'exception (ex: <= 100) tout en intégrant un repli de secours (`min_keep`) si aucune IP n'obtient un rang d'exception.
**Logique**:
- Ajout de `all_under_threshold(threshold)` pour tester si 100% des IP ont obtenu un rang exceptionnel.
- Ajout de `select_adaptive(golden_threshold, min_keep, max_keep)` : conserve toutes les IP ayant un rang <= seuil (garde les 5 si les 5 sont <= 100), mais conserve au minimum le top `min_keep` si aucun ticket n'atteint le seuil, évitant l'abandon total des sessions en cas de forte affluence.
**Attention**:
- Sur un drop à fort trafic (50 000 personnes), la probabilité que 5 IP sur 5 soient dans le top 100 est infinitésimale (~3.2e-14). Le mécanisme de repli est indispensable.

## [2026-10-01] LOTTERY_20_IPS_TEMPLATE
**Fichiers**: `config/lottery_20_ips.json.example`, `examples/demo_lottery_20_ips.py`, `modules/retail/tickets/lottery_selector.py`
**Raison**: Modèle opérationnel et script de démonstration calibré spécifiquement pour 20 adresses IP / proxies.
**Logique**:
- Fichier de configuration exemple `config/lottery_20_ips.json.example` structurant les 20 instances d'IP/proxies avec libellés de comptes et endpoints de file.
- Démonstrateur exécutable `examples/demo_lottery_20_ips.py` simulant l'injection simultanée à T0 sur 20 IP, le classement instantané en 80 ms, et l'affichage d'un tableau récapitulatif coloré (Rich Table).
- Correction de la détection d'instance vide dans `MultiIpLotteryOrchestrator` via `__bool__` et garde explicite `is not None`.
**Attention**:
- Vérifier que les 20 proxies sont configurés en sessions persistantes ("sticky") pour éviter tout changement d'IP durant l'attente dans la file.

## [2026-10-01] GUI_MULTI_IP_TOMBOLA
**Fichiers**: `gui/app.py`
**Raison**: Intégration graphique de la gestion de tombola 20 IP dans l'interface Tkinter pour contrôle visuel et transfert direct vers le sniper.
**Logique**:
- Ajout d'une structure à onglets `ttk.Notebook` isolant le tir T0 et la tombola multi-IP.
- Tableau interactif `ttk.Treeview` affichant en temps réel le classement des 20 IP, les tickets d'or (<= 500), et les sessions élaguées.
- Bouton de transfert en 1 clic injectant la session de l'IP gagnante dans l'onglet de réservation.
**Attention**:
- Le thread UI reste strictement isolé du thread worker asynchrone pour ne pas altérer la précision temporelle à l'ouverture.

## [2026-10-01] HEADLESS_QUEUE_WORKER
**Fichiers**: `modules/retail/tickets/queue_worker.py`, `modules/retail/tickets/__init__.py`, `tests/test_queue_worker.py`
**Raison**: Automatisation complète du passage de file d'attente virtuelle via Google Chrome/Playwright et transmission instantanée du jeton d'admission signé vers le moteur HTTP/2 rapide.
**Logique**:
- Détection automatique du binaire Google Chrome système (`C:\Program Files\Google\Chrome\Application\chrome.exe`) sans téléchargement tiers.
- Lancement de contextes isolés avec proxy dédié et arguments anti-détection (`--disable-blink-features=AutomationControlled`).
- Surveillance asynchrone continue de l'admission (redirection d'URL et apparition de cookies de file signés type `QueueITAccepted`).
- Extraction immédiate des cookies et du User-Agent dans une structure typée `AdmissionHandoff` pour alimenter le moteur d'exécution en moins d'une milliseconde.
**Attention**:
- Chaque instance de worker doit tourner sur un contexte navigateur isolé et un proxy sticky dédié pour éviter l'invalidation de session.

## [2026-10-01] IN_BROWSER_FETCH_EXECUTION
**Fichiers**: `modules/retail/tickets/queue_worker.py`, `tests/test_queue_worker.py`
**Raison**: Éliminer la discordance d'empreinte TLS / HTTP/2 (JA3/JA4) en déclenchant la réservation directement au sein du contexte de page Chrome admis.
**Logique**:
- Implémentation de `execute_in_browser_fetch()` dans `HeadlessQueueWorker`.
- Exécute `window.fetch()` via `page.evaluate()` directement sur la socket et la session TLS du navigateur Chrome réel.
- Préserve 100% de la signature de pile réseau (chiffrement BoringSSL, en-têtes HTTP/2 natifs, cookies de session automatiques).
**Attention**:
- Nécessite que le worker de file soit démarré et que la page soit active.

## [2026-10-01] BROWSER_WORKER_EXECUTOR_INTEGRATION
**Fichiers**: `modules/retail/tickets/ticket_engine.py`, `modules/retail/tickets/queue_worker.py`
**Raison**: Intégration complète du worker de file d'attente Chromium dans le moteur d'exécution `TicketDropExecutor` pour un tir natif in-browser et maintien de session actif.
**Logique**:
- `TicketDropExecutor` accepte désormais une instance `browser_worker`. Si présente, la réservation est injectée directement via `execute_in_browser_fetch()` dans le navigateur Chrome sans passer par `httpx` / OpenSSL.
- `HeadlessQueueWorker` inclut un pulse d'activité naturelle périodique (`_natural_keepalive_loop`) évitant le gel d'onglet en arrière-plan et maintenant la réputation de session.
- Détection proactive des challenges interactifs (`check_interactive_challenge`) pour les formulaires de vérification humaine.
**Attention**:
- Si `browser_worker` n'est pas fourni ou inactif, le moteur bascule automatiquement sur le transport haute performance `PrewarmedHttpClient`.

## [2026-10-01] RANDOMIZED_KEEPALIVE_ANTI_FINGERPRINTING
**Fichiers**: `modules/retail/tickets/queue_worker.py`, `tests/test_queue_worker.py`
**Raison**: Remplacer la périodicité fixe de 15s (détectable par analyse fréquentielle FFT des WAF type Akamai/DataDome) par un jitter aléatoire et des interactions naturelles.
**Logique**:
- Intervalles variables configurables (`keepalive_min_interval_sec=7.0`, `keepalive_max_interval_sec=23.0`) avec distribution uniforme.
- Déplacements de curseur non-linéaires en plusieurs étapes interpolées (`steps=3..8`) et coordonnées aléatoires.
- Micro-scrolls occasionnels (35% de probabilité) pour simuler la consultation inactive d'une page par un utilisateur réel.

## [2026-10-01] FULL_AUTOPILOT_PIPELINE
**Fichiers**: `gui/app.py`, `Lancer_Sniper.bat`
**Raison**: Éliminer l'intégralité de la latence humaine (5 à 15 secondes d'hésitation et de clics manuels) entre la loterie des 20 IPs, la sélection du ticket d'or, l'armement de la socket et le tir T0.
**Logique**:
- Implémentation de `run_autopilot_pipeline()` dans `TicketWorker` : enchaîne sans interruption le tirage 20 IP, le tri adaptatif (< 1 ms), l'injection de cookies, la pré-chauffe HTTP/2 et le tir de réservation immédiat.
- En cas de drop saturé, enclenchement automatique et autonome du rattrapage des paniers abandonnés ("Wave Sniping") sans intervention utilisateur.
- Alerte sonore Windows native (`winsound.MessageBeep`) et ouverture automatique de la page de paiement dès que les places sont verrouillées au panier.
- Ajout du bouton 1-clic `LANCER LE PIPELINE AUTOPILOTE COMPLET` et de la case à cocher d'enchaînement automatique dans l'onglet Tombola.

## [2026-10-01] GUI_PRESETS_DROP_TIME_AND_PERSISTENCE
**Fichiers**: `gui/app.py`, `tests/test_tickets.py`
**Raison**: Permettre la planification exacte à l'heure T0 (`HH:MM:SS`), la bascule Chrome Anti-WAF dans l'interface, les presets plateformes immédiats et la mémorisation des réglages entre sessions.
**Logique**:
- Ajout du parsing d'heure `parse_drop_time_str(HH:MM:SS)` connectant l'interface graphique au scheduler atomique NTP (`wait_until_atomic_timestamp`).
- Intégration de la case à cocher `Mode Chrome Natif (Anti-WAF)` déclenchant `HeadlessQueueWorker` pour un tir `execute_in_browser_fetch` direct.
- Sélecteur de presets billetteries (Shotgun, Roland-Garros, Weezevent, Accor Arena, Fnac Spectacles) remplissant instantanément les URL et catégories.
- Sauvegarde/chargement transparent des paramètres dans `config/gui_settings.json` (ignoré par Git pour protéger les cookies et tokens).
