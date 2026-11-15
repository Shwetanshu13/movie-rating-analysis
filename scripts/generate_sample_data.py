#!/usr/bin/env python3
"""Generate a MovieLens-format sample dataset (movies.csv, ratings.csv).

Real MovieLens (https://grouplens.org/datasets/movielens/) uses the same
schema, so you can drop its movies.csv / ratings.csv into ../data instead.

movies.csv : movieId,title,genres        (genres separated by '|')
ratings.csv: userId,movieId,rating,timestamp
"""
import csv
import os
import random
import sys

GENRES = ["Action", "Adventure", "Animation", "Children", "Comedy", "Crime",
          "Documentary", "Drama", "Fantasy", "Horror", "Musical", "Mystery",
          "Romance", "Sci-Fi", "Thriller", "War", "Western"]
# Rough genre "quality bias" so the results are not all identical.
BIAS = {g: random.Random(g).uniform(-0.5, 0.5) for g in GENRES}


def main(out_dir, n_movies=2000, n_users=3000, n_ratings=200000, seed=42):
    rnd = random.Random(seed)
    os.makedirs(out_dir, exist_ok=True)

    movies = []
    with open(os.path.join(out_dir, "movies.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["movieId", "title", "genres"])
        for mid in range(1, n_movies + 1):
            gs = rnd.sample(GENRES, rnd.randint(1, 3))
            quality = rnd.gauss(3.4, 0.5)
            movies.append((mid, gs, quality))
            w.writerow([mid, f"Movie {mid} ({rnd.randint(1970, 2023)})", "|".join(gs)])

    with open(os.path.join(out_dir, "ratings.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["userId", "movieId", "rating", "timestamp"])
        for _ in range(n_ratings):
            mid, gs, q = movies[rnd.randrange(n_movies)]
            bias = sum(BIAS[g] for g in gs) / len(gs)
            r = round(min(5.0, max(0.5, rnd.gauss(q + bias, 0.9))) * 2) / 2
            w.writerow([rnd.randint(1, n_users), mid, r,
                        rnd.randint(1_000_000_000, 1_700_000_000)])
    print(f"Wrote {n_movies} movies and {n_ratings} ratings to {out_dir}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "data"))
