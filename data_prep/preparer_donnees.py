# -*- coding: utf-8 -*-
"""Préparation du jeu de données Microsoft Azure Predictive Maintenance (Kaggle).

Entrée  : data/azure_pdm/      (5 CSV téléchargés depuis Kaggle, non modifiés)
Sorties : data/prepared/       (historique des 20 machines du projet, format du projet)
          data/echantillon/    (48 premières heures, petit extrait joint au dépôt)

Étapes :
 1. Contrôle qualité : valeurs manquantes, doublons, trous dans la série horaire.
 2. Choix de 20 machines sur 100 : le jeu a 4 modèles, comme nos 4 catégories.
    Pour chaque modèle, les 5 machines qui ont le plus de pannes (historique le plus riche).
 3. Renommage vers les identifiants Odoo (PRESSE-01 … POMPE-05).
 4. Dates au format ISO 8601 UTC (« 2015-01-01T06:00:00Z »), comme le simulateur.
 5. Arrondi des mesures à 2 décimales, tri par machine puis par date.

Les grandeurs du jeu (tension, rotation, pression, vibration) sont conservées telles quelles :
elles ne sont PAS converties en température ou en puissance, ce serait inventer des données.

Usage (dossier Mini Projet Big Data) :
    python -I data_prep/preparer_donnees.py
"""
import csv
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ICI = Path(__file__).resolve().parent.parent
SOURCE = ICI / "data" / "azure_pdm"
SORTIE = ICI / "data" / "prepared"
ECHANTILLON = ICI / "data" / "echantillon"

# Convention du projet : un modèle Azure = une catégorie Odoo.
MODELE_VERS_CATEGORIE = {
    "model1": "PRESSE",
    "model2": "COMP",
    "model3": "CONV",
    "model4": "POMPE",
}
MACHINES_PAR_CATEGORIE = 5
HEURES_ECHANTILLON = 48


def lire(nom):
    with open(SOURCE / nom, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def iso(dt_texte):
    return datetime.strptime(dt_texte, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")


def ecrire(chemin, colonnes, lignes):
    chemin.parent.mkdir(parents=True, exist_ok=True)
    with open(chemin, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(colonnes)
        w.writerows(lignes)
    print(f"  {chemin.relative_to(ICI)} : {len(lignes)} lignes")


def main():
    if not (SOURCE / "PdM_telemetry.csv").exists():
        sys.exit(f"Fichiers absents : décompresser le zip Kaggle dans {SOURCE}")

    machines = lire("PdM_machines.csv")
    pannes = lire("PdM_failures.csv")
    maint = lire("PdM_maint.csv")
    erreurs = lire("PdM_errors.csv")
    telemetrie = lire("PdM_telemetry.csv")

    # ---- 1. Contrôle qualité -------------------------------------------------
    print("1. Contrôle qualité")
    vides = sum(1 for r in telemetrie for v in r.values() if not v.strip())
    doublons = len(telemetrie) - len({(r["datetime"], r["machineID"]) for r in telemetrie})
    par_machine = defaultdict(list)
    for r in telemetrie:
        par_machine[r["machineID"]].append(r["datetime"])
    trous = 0
    for dates in par_machine.values():
        d = sorted(datetime.strptime(x, "%Y-%m-%d %H:%M:%S") for x in dates)
        trous += sum(1 for a, b in zip(d, d[1:]) if b - a != timedelta(hours=1))
    print(f"  télémétrie : {len(telemetrie)} lignes, {len(par_machine)} machines, "
          f"{vides} valeurs vides, {doublons} doublons (date, machine), {trous} trous horaires")
    if vides or doublons or trous:
        sys.exit("Données incomplètes : préparation arrêtée.")

    # ---- 2. Choix des 20 machines --------------------------------------------
    print("2. Choix des machines")
    nb_pannes = Counter(r["machineID"] for r in pannes)
    infos = {r["machineID"]: r for r in machines}
    correspondance = {}   # machineID Azure -> identifiant projet
    lignes_corresp = []
    for modele, prefixe in MODELE_VERS_CATEGORIE.items():
        candidats = [m for m in infos if infos[m]["model"] == modele]
        candidats.sort(key=lambda m: (-nb_pannes[m], int(m)))
        for rang, m in enumerate(sorted(candidats[:MACHINES_PAR_CATEGORIE], key=int), start=1):
            ident = f"{prefixe}-{rang:02d}"
            correspondance[m] = ident
            lignes_corresp.append([ident, m, modele, infos[m]["age"], nb_pannes[m]])
            print(f"  {ident:9} <- machine {m:>3} ({modele}, {infos[m]['age']:>2} ans, {nb_pannes[m]} pannes)")

    # ---- 3 à 5. Conversion ---------------------------------------------------
    print("3. Fichiers préparés")
    ecrire(SORTIE / "correspondance_machines.csv",
           ["machine_id", "azure_machine_id", "azure_model", "age_ans", "nb_pannes_2015"], lignes_corresp)

    tele = []
    for r in telemetrie:
        ident = correspondance.get(r["machineID"])
        if ident:
            tele.append([ident, iso(r["datetime"]),
                         round(float(r["volt"]), 2), round(float(r["rotate"]), 2),
                         round(float(r["pressure"]), 2), round(float(r["vibration"]), 2)])
    tele.sort(key=lambda x: (x[0], x[1]))
    colonnes_tele = ["machine_id", "ts", "volt", "rotate", "pressure", "vibration"]
    ecrire(SORTIE / "telemetry_20_machines.csv", colonnes_tele, tele)

    for nom, src, champ, col in (("failures", pannes, "failure", "composant"),
                                 ("maint", maint, "comp", "composant"),
                                 ("errors", erreurs, "errorID", "erreur")):
        lignes = sorted(([correspondance[r["machineID"]], iso(r["datetime"]), r[champ]]
                         for r in src if r["machineID"] in correspondance),
                        key=lambda x: (x[0], x[1]))
        ecrire(SORTIE / f"{nom}_20_machines.csv", ["machine_id", "ts", col], lignes)

    # Échantillon : 48 premières heures des 20 machines (joint au dépôt, quelques dizaines de Ko)
    debut = min(x[1] for x in tele)
    limite = (datetime.strptime(debut, "%Y-%m-%dT%H:%M:%SZ")
              + timedelta(hours=HEURES_ECHANTILLON)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ecrire(ECHANTILLON / "telemetry_echantillon_48h.csv", colonnes_tele,
           [x for x in tele if x[1] < limite])
    ecrire(ECHANTILLON / "correspondance_machines.csv",
           ["machine_id", "azure_machine_id", "azure_model", "age_ans", "nb_pannes_2015"], lignes_corresp)

    total_pannes = sum(l[4] for l in lignes_corresp)
    print(f"Terminé : 20 machines, {len(tele)} mesures horaires, {total_pannes} pannes en 2015.")


if __name__ == "__main__":
    main()
