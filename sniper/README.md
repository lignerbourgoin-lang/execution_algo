# Sniper - Assistant d'Achat Personnel Haute Performance

Assistant d'achat personnel pour **UNE identité** (1 compte réel, 1 session persistante, respect strict des quotas `account_cap`).

## 1. Installation

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
playwright install chromium
```

## 2. Configuration

1. **`family.yaml`** : Définir la variable `quantity` (ex: `5` pour une famille).
2. **`events.yaml`** : Configurer les événements cibles (`drop`, `resale`, `marketplace`, `after_queue`), vos cookies de session et les URLs réelles d'API.

## 3. Utilisation

```powershell
# Lister les événements configurés
python -m sniper --list

# Mode test (warmup + lecture inventaire sans achat)
python -m sniper --dry-run --event example_drop

# Exécution réelle
python -m sniper --event example_drop
```

## 4. Tests

```powershell
python -m unittest discover tests
```
