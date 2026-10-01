# Changelog Features

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
