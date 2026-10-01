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
