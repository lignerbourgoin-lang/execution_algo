# Architecture & Fonctionnement Technique du Moteur d'Exécution

Ce document détaille l'architecture complète du projet `execution_algo`, les optimisations réseau/système implémentées, ce que le code permet de réaliser, ainsi que ses limites techniques réelles face aux protections modernes des billetteries.

---

## 1. Vue d'Ensemble & Objectifs

Le projet est conçu comme un **moteur d'exécution asynchrone à très haute précision** pour l'acquisition de billets et la surveillance de revente officielle :
1. **Zéro latence inutile côté client** : Réduction du temps de réaction local de ~200 ms à moins de **2 ms**.
2. **Synchronisation d'horloge atomique** : Précision milliseconde alignée sur le temps universel UTC via NTP (RFC 5905).
3. **Pré-connexion réseau (HTTP/2 keep-alive)** : Élimination du handshake TCP et de la négociation TLS au moment du tir ($T_0$).
4. **Surveillance & Récupération** : Rattrapage automatique des paniers expirés (10-15 min après le drop) et alertes instantanées sur smartphone via `ntfy.sh`.
5. **Cadre strict de sécurité & conformité** : Aucune tentative de contournement intrusif de WAF (pas de solveur de CAPTCHA, pas de spoofing d'empreinte navigateur).

---

## 2. Architecture des Modules

```
execution_algo/
├── core/
│   ├── network/
│   │   └── persistent_client.py   # Client HTTP/2 pré-connecté, tir zéro-allocation (send_fast)
│   ├── system.py                  # Optimisations OS : timer Windows 1ms, CPU pinning, GC freeze
│   ├── clock/                     # Horloge de référence
│   ├── rate_limiter/              # Régulation adaptative des requêtes (respect des quotas / 429)
│   └── telemetry/                 # Mesure nanoseconde des étapes de latence
│
├── modules/retail/
│   ├── tickets/
│   │   └── ticket_engine.py       # Moteur de tir T0, cascade multi-catégories, wave release sniper
│   ├── clock/
│   │   └── ntp_sync.py            # Client NTP RFC 5905, attente tri-phase (sleep -> yield -> spin)
│   ├── resale/
│   │   └── watcher.py             # Surveillance revente officielle, auto-cart & push mobile
│   ├── monitors/
│   │   └── conditional_poll.py    # Polling conditionnel (ETag 304, If-None-Match, BLAKE2b)
│   └── notify/
│       └── notifiers.py           # Envoi d'alertes instantanées (ntfy.sh smartphone, navigateur)
│
├── gui/
│   └── app.py                     # Interface graphique réactive (thread UI isolé du moteur d'exécution)
└── tests/                         # Suite de tests unitaires (70 tests validés)
```

---

## 3. Détail des Optimisations Techniques Implémentées

### A. Précision Temporelle & OS (`core/system.py` & `ntp_sync.py`)
* **Résolution d'horloge Windows (Kernel Timer 1 ms)** : Par défaut, le scheduler de Windows a une granularité de 15.6 ms. Le module active `timeBeginPeriod(1)` via l'API Win32 pour forcer une résolution de 1 ms.
* **Attente Tri-Phase (`wait_until_atomic_timestamp`)** :
  1. *Sommeil grossier* (`asyncio.sleep`) jusqu'à $T - 100\text{ ms}$ : CPU à 0%.
  2. *Cession coopérative* (`asyncio.sleep(0)`) jusqu'à $T - 1\text{ ms}$ : évite tout dépassement de quantum.
  3. *Micro-spin final* (`time.perf_counter()`) pendant 1 ms : verrouillage au millième de seconde sans dérive.
* **Affinité CPU (`pin_thread_to_cpu`)** : Épingle le thread d'exécution sur un cœur physique dédié (Core 2) via `SetThreadAffinityMask`, empêchant l'invalidation des caches processeur L1/L2.
* **Gel du ramasse-miettes (`freeze_garbage_collection`)** : Désactive temporairement le Garbage Collector Python (`gc.disable()`) durant la fenêtre critique du tir $T_0$ pour éviter tout arrêt imprévisible (*stop-the-world*).

### B. Couche Réseau & Zéro-Allocation (`core/network/persistent_client.py`)
* **Pré-chauffe TCP/TLS (Keep-Alive)** : La connexion avec le serveur est établie en avance. Au moment $T_0$, aucun temps n'est perdu en résolution DNS (20-50 ms), handshake TCP (15-40 ms) ou négociation TLS 1.3 (30-80 ms).
* **Requête Pré-Compilée (`build_fast_request` & `send_fast`)** :
  L'objet `httpx.Request` binaire (URL, en-têtes normalisés, payload JSON pré-sérialisé) est préparé avant l'ouverture. À $T_0$, le moteur injecte directement les octets sur la socket sans temps CPU de parsing.

### C. Stratégie de Réservation & Retries (`modules/retail/tickets/ticket_engine.py`)
* **Rafale Micro-Burst sur ouverture différée** : Si le serveur ouvre avec quelques dixièmes de seconde de retard (retournant `404`, `425 Too Early`, ou `503`), le moteur enchaîne 5 tentatives espacées de 80 ms sur la socket pré-chauffée. Dès l'obtention du `200 OK`, la rafale s'arrête.
* **Cascade Multi-Catégories** : Si la catégorie souhaitée (`CARRE_OR`) est épuisée (`409 Conflict`), le moteur bascule automatiquement sur les catégories de repli (`CAT_1`, `FOSSE`) en moins de 2 ms.
* **Réservation Parallèle (Hedging)** : Possibilité de tenter plusieurs catégories en simultané via multiplexage HTTP/2 (`execute_parallel_categories`). La première requête qui réserve un panier gagne et annule les autres.

### D. Rattrapage des Paniers Expirés ("Wave Sniping")
* **Cycle d'expiration des paniers** : Sur toutes les billetteries, un panier non payé expire après 10 à 15 minutes.
* **Surveillance par vagues** :
  * Polling standard hors période d'expiration pour respecter les quotas IP.
  * Accélération automatique à 150 ms durant les fenêtres critiques ($T+9.5\text{m} \to T+11.5\text{m}$ et $T+14.5\text{m} \to T+16.5\text{m}$).
  * Résilience aux limites de requêtes : respect des en-têtes `Retry-After` en cas de code `HTTP 429`.

### E. Surveillance de Revente & Alerte Mobile (`watcher.py` & `notifiers.py`)
* **Polling conditionnel (ETag 304 & BLAKE2b)** : Interroge les flux de revente officiels sans consommer de bande passante inutile tant que la page n'a pas changé.
* **Auto-Carting** : Verrouille automatiquement le billet au panier dès détection d'une annonce respectant le prix maximum.
* **Alerte push smartphone (ntfy.sh)** : Envoie une notification prioritaire sur iPhone/Android avec lien cliquable ouvrant directement le paiement.

---

## 4. Ce qui est Faisable vs Les Limites Réelles (Réalité du Marché)

### Ce que le code fait parfaitement :
| Fonctionnalité | Efficacité |
| :--- | :--- |
| **Gains de latence locale** | Gain de 150 à 300 ms par rapport à un navigateur classique ou un script naïf. |
| **Précision de tir à l'ouverture** | Alignement atomique NTP évitant de tirer trop tôt (rejet) ou trop tard. |
| **Surveillance 24/7 de revente officielle** | Réaction sous la seconde dès qu'un fan remet un billet en vente. |
| **Rattrapage des paniers abandonnés** | Capture des places libérées par les échecs de paiement 10 min après l'ouverture. |

### Les limites infranchissables face aux grandes plateformes :
Sur des plateformes massives comme **Ticketmaster, AXS, Fnac Spectacles, See Tickets ou Dice**, la vitesse réseau brute n'est qu'un facteur parmi d'autres :

1. **Salles d'attente virtuelles (Virtual Waiting Rooms / Queue-it)** :
   * *Principe* : Toutes les connexions arrivant avant ou à l'heure du drop sont placées dans une file d'attente et **mélangées de façon aléatoire** (*lottery shuffle*).
   * *Conséquence* : Arriver 5 ms avant ou après tout le monde ne donne aucun ordre de priorité ; le numéro de passage attribué est purement aléatoire côté serveur.
2. **Gestion de réputation et d'empreinte (Cloudflare Turnstile, DataDome, Akamai)** :
   * *Principe* : Analyse de l'empreinte TLS (JA3/JA4), de la pile TCP/IP, des en-têtes HTTP/2 et exécution de challenges JavaScript silencieux pour vérifier qu'un navigateur réel est utilisé.
   * *Conséquence* : Les requêtes HTTP nues (sans navigateur avec exécution JS et rendu graphique) sont interceptées par des pages de blocage (403 / Captcha).
3. **Comptes Verified Fan & Quotas stricts** :
   * *Principe* : Vente réservée aux comptes préalablement vérifiés (code SMS unique, carte bancaire liée, limite de 2 à 4 places par personne physique).
   * *Conséquence* : L'automatisation ne peut pas multiplier les chances au-delà de ce que votre identité et vos autorisations permettent.

---

## 5. Comment Utiliser le Projet Actuel

### Lancer l'interface graphique :
```powershell
# Depuis la racine du projet
C:\Eliott\154\.venv\Scripts\python.exe gui\app.py
```

### Configurer une réservation :
1. Renseigner l'**URL Billetterie** et l'**ID Événement**.
2. Indiquer les catégories par ordre de préférence : ex. `CARRE_OR, CAT_1, FOSSE`.
3. *(Optionnel)* Renseigner le cookie de session de votre compte connecté.
4. *(Optionnel)* Renseigner un topic privé [ntfy.sh](https://ntfy.sh) pour recevoir les alertes sur smartphone.
5. Cliquer sur **1. Pré-chauffer TLS & Horloge**.
6. Cliquer sur **2. Déclencher Réservation** au moment du drop, ou **3. Rattrapage Paniers Expirés** 10 minutes plus tard.
7. Dès que le panier est obtenu, le bouton vert **4. Ouvrir Panier** s'active et redirige vers le paiement 3D-Secure.

### Exécuter les tests unitaires :
```powershell
C:\Eliott\154\.venv\Scripts\python.exe -m unittest discover tests
```
*70 tests unitaires couvrent la totalité des modules (horloge, scheduler, client persistant, retries, paniers expirés, revente).*
