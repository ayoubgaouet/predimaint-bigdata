# Kibana 8.17 : data view et premier tableau de bord PrediMaint

Prérequis : pile démarrée, simulateur lancé depuis quelques minutes. Ouvrir http://localhost:5601.

## 1. Data view

1. Menu ☰ › **Stack Management** › **Data Views** › **Create data view**.
2. Name : `PrediMaint capteurs`.
3. Index pattern : `predimaint-sensors-*` (à droite, l'index du jour doit apparaître).
4. Timestamp field : `@timestamp`.
5. **Save data view to Kibana**.

Contrôle rapide : menu ☰ › **Discover**, data view `PrediMaint capteurs`, période *Last 15 minutes*.
Les documents arrivent ; les champs `machine_id`, `niveau`, `temperature`… ont le bon type
(icône `t` pour keyword, `#` pour les nombres).

## 2. Tableau de bord

Menu ☰ › **Dashboards** › **Create dashboard**. En haut à droite : période **Last 15 minutes**,
puis l'icône d'horloge › **Refresh every 5 seconds** › **Start**.

### Visualisation 1 : température par machine dans le temps

1. **Create visualization** (éditeur Lens), type **Line**.
2. **Horizontal axis** : `@timestamp` (Date histogram, intervalle *Auto* ; ou *Customize time interval* : 10 seconds).
3. **Vertical axis** : `temperature`, fonction **Average**.
4. **Breakdown** : `machine_id`, **Top 20 values** (décocher *Group remaining values as "Other"*).
5. Titre : `Température moyenne par machine`. **Save and return**.

Astuce démo : 20 courbes, c'est beaucoup. Une barre de filtre `categorie : "compresseurs"`
n'affiche que les 5 compresseurs ; la panne de COMP-03 se voit alors immédiatement.

### Visualisation 2 : machines au-dessus du seuil

1. **Create visualization**, type **Table**.
2. Dans la barre de requête de la visualisation (ou *Filter* du calque) :
   `niveau : "surveillance" or niveau : "critique"`.
3. **Rows** : `machine_id` (Top 20 values), puis une 2e ligne `depassements` (Top 5 values).
4. **Metrics** : **Maximum** de `temperature`, **Maximum** de `vibration`, **Count** (renommer « Mesures hors seuil »).
5. Titre : `Machines au-dessus du seuil (15 dernières minutes)`. **Save and return**.

Variante plus visuelle : type **Bar horizontal**, Vertical axis `machine_id`, Horizontal axis
`Count`, Breakdown `niveau` (couleurs : orange pour surveillance, rouge pour critique).

### Visualisation 3 (option) : compteur de mesures critiques

Type **Metric**, **Count**, filtre `niveau : "critique"`, titre `Mesures critiques`.

### Enregistrer

**Save** : titre `PrediMaint - Supervision temps réel`, cocher **Store time with dashboard**
(la période et le rafraîchissement de 5 s sont conservés).

## 3. Démonstration de la panne

1. Le tableau de bord est ouvert, rafraîchi toutes les 5 s, et la table « au-dessus du seuil » est vide.
2. Dans un second terminal :
   `.\.venv\Scripts\python.exe simulateur.py --panne COMP-03 --type surchauffe`
3. Sous 5 à 10 s, la courbe de COMP-03 monte (de 78 °C vers environ 130 °C). COMP-03 apparaît
   dans la table, d'abord en `surveillance` (au-delà de 90 °C), puis en `critique` (au-delà de 105 °C).
4. `--reparer COMP-03` : la courbe redescend ; la machine quitte la table quand ses mesures
   hors seuil sortent de la fenêtre de 15 minutes.

## Requêtes équivalentes (Dev Tools)

```text
GET predimaint-sensors-*/_count

GET predimaint-sensors-*/_search
{
  "size": 0,
  "query": { "bool": { "filter": [
    { "terms": { "niveau": ["surveillance", "critique"] } },
    { "range": { "@timestamp": { "gte": "now-15m" } } } ] } },
  "aggs": { "machines": { "terms": { "field": "machine_id", "size": 20 },
            "aggs": { "t_max": { "max": { "field": "temperature" } } } } }
}
```
