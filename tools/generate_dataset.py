"""Génère les trajets de vélos en libre-service de Lisbonne du 19 octobre 2026.

Trois partenaires, trois formats, trois conventions d'unités :
  Belém              CSV      duration_min, distance_km
  Alfama             JSON     duration_s,   distance_m   (4 trajets en commun avec Belém)
  Parque das Nações  Parquet  duration_min, distance_km
Plus une note d'opérateur en .txt que le pipeline doit ignorer.
"""
import json, random
from pathlib import Path
import pandas as pd

random.seed(1908)
OUT = Path(__file__).resolve().parent.parent / "data"
OUT.mkdir(exist_ok=True)
DAY = "2026-10-19"

STATIONS = {
    "Belém": ["Mosteiro dos Jerónimos", "Torre de Belém", "Padrão dos Descobrimentos", "Pastéis de Belém", "MAAT"],
    "Alfama": ["Miradouro de Santa Luzia", "Sé de Lisboa", "Largo do Chafariz", "Feira da Ladra", "Museu do Fado"],
    "Parque das Nações": ["Oceanário", "Estação Oriente", "Torre Vasco da Gama", "Pavilhão do Conhecimento", "Marina"],
}
PROFILE = {"Belém": (22, 3.4, 0.38), "Alfama": (17, 2.1, 0.62), "Parque das Nações": (25, 4.2, 0.31)}

def trips(district, n, start_id):
    mu_d, mu_k, p_e = PROFILE[district]
    rows = []
    for i in range(n):
        h = random.choices(range(7, 23), weights=[3, 6, 8, 5, 4, 5, 6, 5, 4, 5, 7, 8, 6, 4, 3, 2])[0]
        m = random.randint(0, 59)
        dur = max(4, int(random.gauss(mu_d, 6)))
        km = round(max(0.4, random.gauss(mu_k, 1.0)), 1)
        rows.append({
            "trip_id": f"LX-{start_id + i:05d}",
            "station": random.choice(STATIONS[district]),
            "district": district,
            "start_time": f"{DAY} {h:02d}:{m:02d}:00",
            "duration_min": dur,
            "distance_km": km,
            "bike_type": "electrique" if random.random() < p_e else "classique",
        })
    return sorted(rows, key=lambda r: r["start_time"])

belem = trips("Belém", 84, 10001)
alfama_own = trips("Alfama", 62, 20001)
parque = trips("Parque das Nações", 90, 30001)

# 4 trajets partis de Belém et arrivés à Alfama : déclarés par les deux partenaires
shared = random.sample(belem, 4)

# Belém : CSV, minutes et kilomètres, tout en texte
pd.DataFrame(belem).to_csv(OUT / f"belem_{DAY}.csv", index=False, encoding="utf-8")

# Alfama : JSON, secondes et mètres
def to_alfama(r):
    return {"trip_id": r["trip_id"], "station": r["station"], "district": r["district"], "start_time": r["start_time"],
            "duration_s": r["duration_min"] * 60, "distance_m": int(round(r["distance_km"] * 1000)), "bike_type": r["bike_type"]}
alfama = sorted([to_alfama(r) for r in alfama_own + shared], key=lambda r: r["start_time"])
(OUT / f"alfama_{DAY}.json").write_text(json.dumps(alfama, ensure_ascii=False, indent=1), encoding="utf-8")

# Parque das Nações : Parquet typé
pq = pd.DataFrame(parque).astype({"duration_min": "float64", "distance_km": "float64"})
pq.to_parquet(OUT / f"parque_{DAY}.parquet", index=False)

(OUT / "notes_operateur.txt").write_text(
    "Note de l'opérateur, 19 octobre 2026 : station Marina fermée de 14 h à 15 h pour maintenance.\n", encoding="utf-8")

print(len(belem), len(alfama), len(parque))
