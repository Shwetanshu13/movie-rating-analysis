#!/usr/bin/env python3
"""Flask dashboard for the Movie Rating Analysis results."""
import csv
import json
import os

from flask import Flask, jsonify, render_template

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RESULTS = os.environ.get("RESULTS_DIR", os.path.join(BASE, "results"))

app = Flask(__name__)


def load_spark():
    p = os.path.join(RESULTS, "spark_results.json")
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return None


def load_mapreduce():
    p = os.path.join(RESULTS, "mapreduce_results.tsv")
    rows = []
    if os.path.exists(p):
        with open(p) as f:
            for r in csv.reader(f, delimiter="\t"):
                if len(r) == 3:
                    rows.append({"genre": r[0], "avg_rating": float(r[1]),
                                 "num_ratings": int(r[2])})
        rows.sort(key=lambda x: -x["avg_rating"])
    return rows


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/results")
def api_results():
    return jsonify({"spark": load_spark(), "mapreduce": load_mapreduce()})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
