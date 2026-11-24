# -*- coding: utf-8 -*-
"""Charge les seuils des capteurs et la fiche des machines dans Redis.

Deux sources possibles :
  --odoo     : lit les seuils et les machines dans Odoo par XML-RPC (synchronisation réelle,
               cahier des charges section 9.1). Paramètres : ODOO_URL (défaut http://localhost:8070),
               ODOO_DB (predimaint), ODOO_USER (admin), ODOO_KEY (sinon demandé au clavier).
  (défaut)   : lit redis/seuils_reference.json, copie des seuils d'Odoo, pour travailler sans Odoo.

Écrit dans Redis (via redis-cli dans le conteneur, aucune bibliothèque à installer) :
  threshold:<categorie>  hash  « <grandeur>:alerte » et « <grandeur>:critique »
  machine:<machine_id>   hash  « categorie » et « equipe »

Usage (dossier Mini Projet Big Data) :
  python redis/charger_seuils.py
  python redis/charger_seuils.py --odoo
"""
import argparse
import getpass
import json
import os
import subprocess
import sys
import xmlrpc.client
from pathlib import Path

ICI = Path(__file__).resolve().parent
PROJET = ICI.parent
COMPOSE = PROJET / "docker-compose.yaml"


def depuis_fichier():
    data = json.loads((ICI / "seuils_reference.json").read_text(encoding="utf-8"))
    seuils = {cat: {g: tuple(v) for g, v in grandeurs.items()} for cat, grandeurs in data["seuils"].items()}
    machines = {}
    for prefixe, info in data["machines"].items():
        for i in range(1, 6):
            machines[f"{prefixe}-{i:02d}"] = info
    return seuils, machines


def depuis_odoo():
    url = os.environ.get("ODOO_URL", "http://localhost:8070")
    db = os.environ.get("ODOO_DB", "predimaint")
    user = os.environ.get("ODOO_USER", "admin")
    key = os.environ.get("ODOO_KEY") or getpass.getpass(f"Mot de passe ou clé d'API de {user} : ")
    uid = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common").authenticate(db, user, key, {})
    if not uid:
        sys.exit("Authentification Odoo refusée.")
    odoo = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

    def lire(modele, domaine, champs):
        return odoo.execute_kw(db, uid, key, modele, "search_read", [domaine], {"fields": champs})

    seuils = {}
    for s in lire("predimaint.sensor.threshold", [], ["category_id", "measure_type",
                                                     "alert_threshold", "critical_threshold"]):
        cat = s["category_id"][1].lower()
        seuils.setdefault(cat, {})[s["measure_type"]] = (s["alert_threshold"], s["critical_threshold"])
    machines = {}
    for e in lire("maintenance.equipment", [("sensor_ref", "!=", False)],
                  ["sensor_ref", "category_id", "maintenance_team_id"]):
        machines[e["sensor_ref"]] = {
            "categorie": e["category_id"][1].lower() if e["category_id"] else "inconnue",
            "equipe": e["maintenance_team_id"][1] if e["maintenance_team_id"] else "",
        }
    return seuils, machines


def commandes_redis(seuils, machines):
    lignes = []
    for cat, grandeurs in sorted(seuils.items()):
        lignes.append(f'DEL "threshold:{cat}"')
        champs = []
        for g, (alerte, critique) in sorted(grandeurs.items()):
            champs += [f"{g}:alerte", f"{alerte:g}", f"{g}:critique", f"{critique:g}"]
        lignes.append(f'HSET "threshold:{cat}" ' + " ".join(f'"{c}"' for c in champs))
    for m, info in sorted(machines.items()):
        lignes.append(f'HSET "machine:{m}" "categorie" "{info["categorie"]}" "equipe" "{info["equipe"]}"')
    return "\n".join(lignes) + "\n"


def main():
    p = argparse.ArgumentParser(description="Charge seuils et machines dans Redis")
    p.add_argument("--odoo", action="store_true", help="lire les seuils dans Odoo (XML-RPC)")
    args = p.parse_args()

    seuils, machines = depuis_odoo() if args.odoo else depuis_fichier()
    source = "Odoo (XML-RPC)" if args.odoo else "seuils_reference.json"
    script = commandes_redis(seuils, machines)
    r = subprocess.run(["docker", "compose", "-f", str(COMPOSE), "exec", "-T", "redis", "redis-cli"],
                       input=script.encode("utf-8"), capture_output=True)
    if r.returncode != 0:
        sys.exit("Échec de redis-cli : " + r.stderr.decode("utf-8", "replace"))
    erreurs = [l for l in r.stdout.decode("utf-8", "replace").splitlines() if l.startswith("ERR")]
    if erreurs:
        sys.exit("Erreurs Redis : " + "; ".join(erreurs))
    nb_seuils = sum(len(g) for g in seuils.values())
    print(f"Source : {source}")
    print(f"Redis : {len(seuils)} catégories ({nb_seuils} seuils) et {len(machines)} machines chargées.")


if __name__ == "__main__":
    main()
