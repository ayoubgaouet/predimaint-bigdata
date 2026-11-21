# -*- coding: utf-8 -*-
"""Simulateur IoT PrediMaint : 20 machines -> Kafka (topic sensor.readings).

Chaque grandeur = valeur nominale + cycle de production + bruit aléatoire (+ dérive lente
optionnelle, + panne injectée). La clé Kafka est machine_id : toutes les mesures d'une
machine vont dans la même partition, donc restent dans l'ordre.

Exemples (PowerShell, depuis Mini Projet Big Data\\simulator, venv activé) :
    python simulateur.py                                   # 20 msg/s (1 par machine et par seconde)
    python simulateur.py --rate 1000                       # test de débit
    python simulateur.py --derive POMPE-02                 # usure lente sur POMPE-02
    python simulateur.py --panne COMP-03 --type surchauffe # dans un 2e terminal : panne en direct
    python simulateur.py --reparer COMP-03                 # fin de la panne
"""
import argparse
import json
import math
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ICI = Path(__file__).resolve().parent
FICHIER_COMMANDES = ICI / "commandes_pannes.json"   # pannes demandées par un 2e terminal
FICHIER_VIE = ICI / ".simulateur_actif"             # « battement de cœur » du simulateur

# Identifiants identiques aux équipements Odoo (module predimaint).
MACHINES = ([f"PRESSE-{i:02d}" for i in range(1, 6)] + [f"COMP-{i:02d}" for i in range(1, 6)]
            + [f"CONV-{i:02d}" for i in range(1, 6)] + [f"POMPE-{i:02d}" for i in range(1, 6)])

# Valeurs nominales par catégorie, toutes sous les seuils d'alerte saisis dans Odoo.
# (nominal, amplitude du cycle, écart-type du bruit, nombre de décimales)
PROFILS = {
    "PRESSE": {"periode_s": 30, "temperature": (48.0, 3.0, 0.6, 1), "vibration": (2.6, 0.5, 0.15, 2),
               "pression": (150.0, 12.0, 2.0, 1), "puissance_kw": (36.0, 4.0, 0.8, 1),
               "bruit_db": (78.0, 2.0, 0.8, 1)},
    "COMP": {"periode_s": 120, "temperature": (76.0, 3.0, 0.7, 1), "vibration": (2.4, 0.3, 0.12, 2),
             "pression": (7.2, 0.4, 0.08, 2), "puissance_kw": (27.0, 2.0, 0.5, 1),
             "bruit_db": (72.0, 1.5, 0.6, 1)},
    # Convoyeurs : pression = tendeur pneumatique de bande (pas de seuil dans Odoo).
    "CONV": {"periode_s": 45, "temperature": (52.0, 2.0, 0.5, 1), "vibration": (1.4, 0.2, 0.08, 2),
             "pression": (6.0, 0.2, 0.05, 2), "puissance_kw": (5.8, 0.6, 0.15, 2),
             "bruit_db": (66.0, 1.5, 0.6, 1)},
    "POMPE": {"periode_s": 60, "temperature": (50.0, 2.0, 0.5, 1), "vibration": (1.6, 0.2, 0.08, 2),
              "pression": (4.6, 0.3, 0.06, 2), "puissance_kw": (12.0, 1.0, 0.3, 1),
              "bruit_db": (70.0, 1.5, 0.6, 1)},
}
GRANDEURS = ["temperature", "vibration", "pression", "puissance_kw", "bruit_db"]

# Effet d'une panne une fois complètement installée : grandeur -> facteur multiplicatif.
# Les valeurs finales dépassent le seuil critique de chaque catégorie.
PANNES = {
    "surchauffe": {"temperature": 1.75, "puissance_kw": 1.15},
    "vibration": {"vibration": 3.6, "bruit_db": 1.18},
    "pression": {"pression": 1.45, "puissance_kw": 1.10},   # surpression
}


def categorie(machine_id):
    return machine_id.split("-")[0]


class Machine:
    def __init__(self, machine_id, rng):
        self.id = machine_id
        self.profil = PROFILS[categorie(machine_id)]
        self.phase = rng.uniform(0, 2 * math.pi)   # les machines ne sont pas synchronisées
        self.derive_depuis = None                   # instant de début de la dérive
        self.panne = None                           # (type, instant de début)

    def mesurer(self, t, rng, derive_pct_min, montee_s):
        """Calcule une mesure à l'instant t (secondes)."""
        cycle = math.sin(2 * math.pi * t / self.profil["periode_s"] + self.phase)
        mesure = {}
        for g in GRANDEURS:
            nominal, amplitude, bruit, decimales = self.profil[g]
            valeur = nominal + amplitude * cycle + rng.gauss(0, bruit)
            # Dérive lente (usure) : +derive_pct_min % du nominal par minute,
            # sur la température et la vibration.
            if self.derive_depuis is not None and g in ("temperature", "vibration"):
                minutes = (t - self.derive_depuis) / 60
                valeur += nominal * derive_pct_min / 100 * minutes
            # Panne : montée progressive vers la valeur de panne.
            if self.panne:
                type_panne, debut = self.panne
                facteur = PANNES[type_panne].get(g)
                if facteur:
                    progression = min(1.0, (t - debut) / montee_s)
                    valeur += nominal * (facteur - 1) * progression
            mesure[g] = round(max(valeur, 0.0), decimales)
        return mesure


def horodatage_iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def simulateur_actif():
    """Vrai si un simulateur tourne déjà (son fichier de vie a moins de 3 s)."""
    try:
        return time.time() - FICHIER_VIE.stat().st_mtime < 3
    except FileNotFoundError:
        return False


def lire_commandes():
    try:
        return json.loads(FICHIER_COMMANDES.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def ecrire_commandes(commandes):
    tmp = FICHIER_COMMANDES.with_suffix(".tmp")
    tmp.write_text(json.dumps(commandes, indent=2), encoding="utf-8")
    os.replace(tmp, FICHIER_COMMANDES)   # écriture atomique : jamais de fichier à moitié écrit


def verifier_machine(machine_id):
    machine_id = machine_id.upper()
    if machine_id != "TOUT" and machine_id not in MACHINES:
        sys.exit(f"Machine inconnue : {machine_id}. Machines : {', '.join(MACHINES)}")
    return machine_id


def creer_producteur(bootstrap):
    from confluent_kafka import Producer
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "simulateur-predimaint",
        "acks": "all",                 # le broker confirme l'écriture avant de passer à la suite
        "enable.idempotence": True,    # pas de doublon si un envoi est réessayé
        "linger.ms": 20,               # regroupe les messages en lots : meilleur débit
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500000,
    })


def main():
    p = argparse.ArgumentParser(description="Simulateur de capteurs PrediMaint -> Kafka")
    p.add_argument("--bootstrap", default="localhost:29092", help="Kafka vu depuis Windows")
    p.add_argument("--topic", default="sensor.readings")
    p.add_argument("--rate", type=float, default=20,
                   help="débit total en messages/s (défaut 20 = 1 message par machine et par seconde)")
    p.add_argument("--duree", type=float, default=0, help="durée en secondes (0 = sans fin)")
    p.add_argument("--machines", help="liste réduite, ex. PRESSE-01,COMP-03 (défaut : les 20)")
    p.add_argument("--derive", nargs="*", default=[], metavar="MACHINE",
                   help="machines en usure lente (dérive de la température et de la vibration)")
    p.add_argument("--derive-pct", type=float, default=1.0,
                   help="vitesse de dérive en %% du nominal par minute (défaut 1)")
    p.add_argument("--panne", metavar="MACHINE", help="machine sur laquelle injecter une panne")
    p.add_argument("--type", dest="type_panne", choices=sorted(PANNES), default="surchauffe")
    p.add_argument("--reparer", metavar="MACHINE", help="supprime la panne (TOUT = toutes les machines)")
    p.add_argument("--montee", type=float, default=15, help="durée de montée d'une panne en secondes")
    p.add_argument("--seed", type=int, help="graine aléatoire (résultats reproductibles)")
    p.add_argument("--sec", action="store_true", help="n'envoie rien, affiche les messages (test)")
    args = p.parse_args()

    # --- Mode « télécommande » : un simulateur tourne déjà, on lui transmet l'ordre et on sort.
    if (args.panne or args.reparer) and simulateur_actif():
        commandes = lire_commandes()
        if args.panne:
            m = verifier_machine(args.panne)
            commandes[m] = args.type_panne
            print(f"Panne « {args.type_panne} » transmise au simulateur en cours pour {m}.")
        if args.reparer:
            m = verifier_machine(args.reparer)
            if m == "TOUT":
                commandes = {}
            else:
                commandes.pop(m, None)
            print(f"Réparation transmise au simulateur en cours pour {m}.")
        ecrire_commandes(commandes)
        return

    rng = random.Random(args.seed)
    ids = [verifier_machine(m) for m in args.machines.split(",")] if args.machines else MACHINES
    machines = {m: Machine(m, rng) for m in ids}
    t0 = time.time()
    for m in args.derive:
        machines[verifier_machine(m)].derive_depuis = 0.0
    commandes = {}
    if args.panne:
        commandes[verifier_machine(args.panne)] = args.type_panne
    ecrire_commandes(commandes)

    producteur = None if args.sec else creer_producteur(args.bootstrap)
    erreurs = [0]

    def accuse(err, _msg):
        if err is not None:
            erreurs[0] += 1

    arret = [False]
    signal.signal(signal.SIGINT, lambda *_: arret.__setitem__(0, True))

    tours_par_s = args.rate / len(machines)
    intervalle = 1.0 / tours_par_s
    print(f"Simulateur : {len(machines)} machines, {args.rate:g} msg/s -> "
          f"{'(sec)' if args.sec else args.bootstrap} / {args.topic}. Ctrl+C pour arrêter.")

    envoyes, dernier_bilan, envoyes_bilan = 0, time.time(), 0
    prochain = time.time()
    derniere_lecture = 0.0
    while not arret[0]:
        maintenant = time.time()
        t = maintenant - t0
        if args.duree and t >= args.duree:
            break

        # Toutes les 0,5 s : applique les pannes demandées (fichier de commandes)
        # et met à jour le fichier de vie.
        if maintenant - derniere_lecture >= 0.5:
            derniere_lecture = maintenant
            demandes = lire_commandes()
            for m, machine in machines.items():
                voulu = demandes.get(m)
                if voulu and (machine.panne is None or machine.panne[0] != voulu):
                    machine.panne = (voulu, t)
                    print(f"  >>> PANNE {voulu} injectée sur {m}")
                elif not voulu and machine.panne:
                    machine.panne = None
                    print(f"  <<< {m} réparée")
            FICHIER_VIE.touch()

        ts = horodatage_iso(maintenant)
        for m, machine in machines.items():
            message = {"machine_id": m, "ts": ts, **machine.mesurer(t, rng, args.derive_pct, args.montee)}
            charge = json.dumps(message, separators=(",", ":"))
            if producteur is None:
                print(charge)
            else:
                while True:
                    try:
                        producteur.produce(args.topic, key=m, value=charge, on_delivery=accuse)
                        break
                    except BufferError:          # file locale pleine : on laisse partir les lots
                        producteur.poll(0.05)
            envoyes += 1
        if producteur is not None:
            producteur.poll(0)

        if time.time() - dernier_bilan >= 5:
            debit = (envoyes - envoyes_bilan) / (time.time() - dernier_bilan)
            pannes = ", ".join(f"{m}:{mc.panne[0]}" for m, mc in machines.items() if mc.panne) or "aucune"
            print(f"[{datetime.now():%H:%M:%S}] {envoyes} messages, {debit:,.0f} msg/s, "
                  f"erreurs {erreurs[0]}, pannes : {pannes}")
            dernier_bilan, envoyes_bilan = time.time(), envoyes

        prochain += intervalle
        attente = prochain - time.time()
        if attente > 0:
            time.sleep(attente)
        else:
            prochain = time.time()   # en retard (débit trop élevé pour le PC) : on ne rattrape pas

    if producteur is not None:
        restants = producteur.flush(10)
        print(f"Arrêt : {envoyes} messages produits, {erreurs[0]} erreurs, {restants} non confirmés.")
    try:
        FICHIER_VIE.unlink()
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    main()
