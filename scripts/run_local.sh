#!/usr/bin/env bash
# Local test run (no cluster needed): emulates Hadoop Streaming with a shell pipe.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
[ -f data/ratings.csv ] || python3 scripts/generate_sample_data.py data

echo "== MapReduce (local pipe: map | sort | reduce) =="
( cd data && cat ratings.csv | python3 ../mapreduce/mapper.py | sort | python3 ../mapreduce/reducer.py ) \
  > results/mapreduce_results.tsv
cat results/mapreduce_results.tsv

echo "== Spark (local[*]) =="
python3 spark/genre_avg.py data/movies.csv data/ratings.csv results/
