# Structures Redis du projet PrediMaint

Redis garde ce qui doit être lu **très vite** et souvent : la dernière valeur de chaque capteur, les
seuils et les alertes en cours. Les données de référence (l'historique complet) sont dans Cassandra.

| Clé | Type Redis | Contenu | Écrit par | Lu par |
| --- | --- | --- | --- | --- |
| `threshold:<categorie>` | hash | `<grandeur>:alerte` et `<grandeur>:critique`, ex. `temperature:alerte = 90` | `redis/charger_seuils.py` (copie des seuils d'Odoo) | Spark Streaming (détection) |
| `machine:<machine_id>` | hash | catégorie, équipe | `redis/charger_seuils.py` | Spark Streaming |
| `last:<machine_id>` | hash | dernières valeurs : `ts`, `temperature`, `vibration`… | Spark Streaming (S18) | Connecteur, tableau de bord |
| `alerts:active` | ensemble trié (zset) | identifiants d'alerte ; score = horodatage | Spark Streaming (S18) | Connecteur Odoo |
| `alerts` | canal Pub/Sub | notification d'une nouvelle alerte | Spark Streaming (S18) | Connecteur Odoo, sans attendre |

## Pourquoi ces types

- **hash** : un objet avec plusieurs champs, lu ou modifié champ par champ (comme une ligne de tableau).
- **ensemble trié** : les alertes sont rangées par date ; on lit les plus récentes en une commande.
- **Pub/Sub** : le connecteur est prévenu immédiatement d'une alerte, au lieu d'interroger Redis en boucle.

## Exemples (redis-cli)

```text
HGETALL threshold:compresseurs
HGET threshold:compresseurs temperature:critique
HGETALL machine:COMP-03
KEYS threshold:*
```
