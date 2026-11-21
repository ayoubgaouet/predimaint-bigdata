# Dossier data

| Sous-dossier | Contenu | Dans le dépôt ? |
| --- | --- | --- |
| `azure_pdm/` | Les 5 CSV du jeu Kaggle *Microsoft Azure Predictive Maintenance*, non modifiés | Non : licence inconnue, 80 Mo, retéléchargeable |
| `prepared/` | Historique 2015 des 20 machines du projet (sortie de `data_prep/preparer_donnees.py`) | Non : régénéré par le script |
| `echantillon/` | Les 48 premières heures des 20 machines et la table de correspondance | Oui |

Le dossier est aussi monté dans Logstash (`/usr/share/logstash/data-in`) pour le futur pipeline « history ».
Description complète, contrôle qualité et choix des machines : `docs/donnees.md`.
