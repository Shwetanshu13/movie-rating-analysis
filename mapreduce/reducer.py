#!/usr/bin/env python3
"""Hadoop Streaming reducer: (genre, rating) sorted by genre -> genre,avg,count."""
import sys

current, total, count = None, 0.0, 0


def emit():
    if current is not None and count:
        print(f"{current}\t{total / count:.4f}\t{count}")


for line in sys.stdin:
    genre, _, rating = line.rstrip("\n").partition("\t")
    try:
        r = float(rating)
    except ValueError:
        continue
    if genre != current:
        emit()
        current, total, count = genre, 0.0, 0
    total += r
    count += 1
emit()
