#!/usr/bin/env python3
"""Spark job: average rating per genre (plus count and a top-movies table).

Usage:
  spark-submit --master yarn spark/genre_avg.py \
      hdfs:///movielens/movies.csv hdfs:///movielens/ratings.csv results/
  # local test:
  python3 spark/genre_avg.py data/movies.csv data/ratings.csv results/
"""
import json
import os
import sys

from pyspark.sql import SparkSession, functions as F


def main(movies_path, ratings_path, out_dir):
    spark = (SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    movies = spark.read.csv(movies_path, header=True, inferSchema=True)
    ratings = spark.read.csv(ratings_path, header=True, inferSchema=True)

    # One row per (movie, genre)
    movie_genre = movies.select(
        "movieId", "title", F.explode(F.split("genres", r"\|")).alias("genre")
    ).filter(F.col("genre") != "(no genres listed)")

    joined = ratings.join(movie_genre, "movieId")

    genre_stats = (joined.groupBy("genre")
                   .agg(F.round(F.avg("rating"), 3).alias("avg_rating"),
                        F.count("rating").alias("num_ratings"))
                   .orderBy(F.desc("avg_rating")))

    top_movies = (ratings.groupBy("movieId")
                  .agg(F.round(F.avg("rating"), 3).alias("avg_rating"),
                       F.count("rating").alias("num_ratings"))
                  .filter(F.col("num_ratings") >= 50)
                  .join(movies.select("movieId", "title", "genres"), "movieId")
                  .orderBy(F.desc("avg_rating"))
                  .limit(10))

    summary = ratings.agg(F.count("*").alias("n"),
                          F.round(F.avg("rating"), 3).alias("avg"),
                          F.countDistinct("userId").alias("users"),
                          F.countDistinct("movieId").alias("movies")).first()

    result = {
        "genres": [r.asDict() for r in genre_stats.collect()],
        "top_movies": [r.asDict() for r in top_movies.collect()],
        "summary": {"total_ratings": summary["n"], "overall_avg": summary["avg"],
                    "users": summary["users"], "movies_rated": summary["movies"]},
    }
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "spark_results.json"), "w") as f:
        json.dump(result, f, indent=2, default=str)
    print("Wrote", os.path.join(out_dir, "spark_results.json"))
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    main(*sys.argv[1:])
