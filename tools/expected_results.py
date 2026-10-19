"""Calcule les résultats attendus de SJD_Lisboa (arrondi demi-supérieur, comme Spark)."""
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import pandas as pd

D = Path(__file__).resolve().parent.parent / "data"
r1 = lambda x: float(Decimal(repr(float(x))).quantize(Decimal("0.1"), ROUND_HALF_UP))
c = pd.read_csv(D / "belem_2026-10-19.csv")
j = pd.DataFrame(json.loads((D / "alfama_2026-10-19.json").read_text(encoding="utf-8")))
j["duration_min"] = j.duration_s / 60; j["distance_km"] = j.distance_m / 1000
p = pd.read_parquet(D / "parque_2026-10-19.parquet")
cols = ["trip_id", "station", "district", "start_time", "duration_min", "distance_km", "bike_type"]
raw = pd.concat([c[cols], j[cols], p[cols]])
t = raw.drop_duplicates("trip_id")
print("lignes lues", len(raw), "trajets", len(t), "doublons", len(raw) - len(t))
g = t.groupby("district").agg(trips=("trip_id", "count"), avg=("duration_min", "mean"), km=("distance_km", "sum"),
                              el=("bike_type", lambda s: (s == "electrique").mean() * 100)).sort_values("trips", ascending=False)
for d, r in g.iterrows():
    print(f"{d:20s} {int(r.trips):4d} {r1(r.avg):6} {r1(r.km):7} {r1(r.el):5}")
print("km total", r1(t.distance_km.sum()), "électrique %", r1((t.bike_type == "electrique").mean() * 100))
print("par format :", len(c), len(j), len(p))
