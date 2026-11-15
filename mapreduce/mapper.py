#!/usr/bin/env python3
"""Hadoop Streaming mapper: ratings.csv line -> (genre, rating) pairs.

movies.csv is shipped to every node with `-files movies.csv` (distributed
cache) and joined in memory (map-side join, since movies.csv is small).
"""
import csv
import sys

movie_genres = {}
with open("movies.csv", newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader, None)
    for row in reader:
        if len(row) >= 3:
            movie_genres[row[0]] = row[-1].split("|")

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) < 3 or parts[0] == "userId":
        continue
    movie_id, rating = parts[1], parts[2]
    try:
        float(rating)
    except ValueError:
        continue
    for genre in movie_genres.get(movie_id, []):
        if genre and genre != "(no genres listed)":
            print(f"{genre}\t{rating}")
