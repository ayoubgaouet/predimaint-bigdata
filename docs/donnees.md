# Données du projet

Le projet utilise deux sources de données complémentaires (cahier des charges, section 8) :

| Source | Rôle | Pourquoi |
| --- | --- | --- |
| **Simulateur** (`simulator/simulateur.py`) | Flux temps réel, une mesure par machine et par seconde | On contrôle les pannes (démonstration) et le débit (tests de performance). |
| **Jeu public Microsoft Azure Predictive Maintenance** (Kaggle) | Historique d'un an : mesures, erreurs, interventions, **pannes réelles datées** | Base pour le traitement batch (MTBF, disponibilité, score de santé) et pour un modèle de prédiction. |

## Jeu de données Microsoft Azure Predictive Maintenance

| Élément | Valeur |
| --- | --- |
| Page | https://www.kaggle.com/datasets/arnabbiswas1/microsoft-azure-predictive-maintenance |
| Origine | Publié par Microsoft en 2016 (*Predictive Maintenance Modelling Guide*), republié sur Kaggle en 2020 |
| Nature | **Données synthétiques** : fabriquées par Microsoft, ce ne sont pas des mesures d'une vraie usine |
| Licence | Indiquée « Unknown » sur Kaggle : les données brutes ne sont donc **pas redistribuées** dans le dépôt |
| Téléchargé le | 07/11/2026, version 3, zip de 31 Mo (80,4 Mo décompressés) |

### Les 5 fichiers (dans `data/azure_pdm/`, non modifiés)

| Fichier | Lignes | Contenu |
| --- | --- | --- |
| `PdM_telemetry.csv` | 876 100 | Moyenne horaire de `volt`, `rotate`, `pressure`, `vibration` pour 100 machines, du 01/01/2015 au 01/01/2016 |
| `PdM_errors.csv` | 3 919 | Erreurs non bloquantes (`error1` à `error5`) |
| `PdM_maint.csv` | 3 286 | Remplacements de composants (`comp1` à `comp4`), préventifs et correctifs, 2014 et 2015 |
| `PdM_failures.csv` | 761 | Pannes : remplacement d'un composant après une panne (sous-ensemble de `maint`) |
| `PdM_machines.csv` | 100 | Modèle (`model1` à `model4`) et âge de chaque machine |

### Contrôle qualité

| Contrôle | Résultat |
| --- | --- |
| Valeurs manquantes | 0 dans les 5 fichiers |
| Lignes en double | 0 |
| Série horaire | 8 761 mesures par machine, aucun trou |
| Plages de valeurs | `volt` 97 à 255 (moy. 171) · `rotate` 138 à 695 (moy. 447) · `pressure` 51 à 186 (moy. 101) · `vibration` 15 à 77 (moy. 40) |
| Pannes | 761 pannes sur 98 machines ; composants : comp2 259, comp1 192, comp4 179, comp3 131 |

## Préparation (`data_prep/preparer_donnees.py`)

1. **Contrôle qualité** automatique : le script s'arrête si une donnée manque.
2. **Choix de 20 machines sur 100.** Le jeu a 4 modèles, comme nos 4 catégories Odoo. Pour chaque modèle, on prend les 5 machines qui ont **le plus de pannes**, donc l'historique le plus riche pour calculer le MTBF.
3. **Renommage** vers les identifiants Odoo (convention : model1 = presses, model2 = compresseurs, model3 = convoyeurs, model4 = pompes).
4. **Dates** au format ISO 8601 UTC (`2015-01-01T06:00:00Z`), comme le simulateur.
5. **Mesures** arrondies à 2 décimales, triées par machine puis par date.

| Machine projet | Machine Azure | Modèle | Âge | Pannes 2015 |
| --- | --- | --- | --- | --- |
| PRESSE-01 … 05 | 13, 17, 22, 37, 99 | model1 | 14 à 16 ans | 13, 15, 15, 14, 19 |
| COMP-01 … 05 | 20, 71, 73, 90, 98 | model2 | 2 à 20 ans | 13, 13, 12, 12, 16 |
| CONV-01 … 05 | 7, 33, 43, 75, 79 | model3 | 14 à 20 ans | 10, 9, 10, 9, 10 |
| POMPE-01 … 05 | 9, 32, 51, 83, 88 | model4 | 7 à 19 ans | 9, 10, 10, 14, 10 |

### Fichiers produits

| Fichier | Lignes | Dans le dépôt ? |
| --- | --- | --- |
| `data/prepared/correspondance_machines.csv` | 20 | non (régénéré par le script) |
| `data/prepared/telemetry_20_machines.csv` | 175 220 (9,7 Mo) | non |
| `data/prepared/failures_20_machines.csv` | 243 | non |
| `data/prepared/maint_20_machines.csv` | 671 | non |
| `data/prepared/errors_20_machines.csv` | 878 | non |
| `data/echantillon/telemetry_echantillon_48h.csv` | 960 (48 premières heures) | **oui** (53 Ko) |
| `data/echantillon/correspondance_machines.csv` | 20 | **oui** |

### Reproduire

1. Télécharger le zip depuis la page Kaggle (compte Kaggle nécessaire).
2. Le décompresser dans `data/azure_pdm/`.
3. Depuis le dossier `Mini Projet Big Data` :

```powershell
py -3.12 -I data_prep\preparer_donnees.py
```

## Limites à connaître

- **Données synthétiques** : utiles pour construire et tester la chaîne, pas pour conclure sur une vraie usine.
- **Grandeurs différentes du simulateur** : le jeu mesure tension, rotation, pression et vibration ; le simulateur mesure température, vibration, pression, puissance et bruit. On garde les grandeurs du jeu telles quelles : les convertir serait inventer des données.
- **Une mesure par heure** (le simulateur envoie une mesure par seconde).
- **Correspondance modèle ↔ catégorie** : c'est une convention du projet, pas une information du jeu.
