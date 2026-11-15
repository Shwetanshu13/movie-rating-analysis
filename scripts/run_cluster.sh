#!/usr/bin/env bash
# Run both jobs on the Hadoop/YARN cluster, then pull results for the UI.
# Run on the MASTER node after start-dfs.sh and start-yarn.sh.
set -euo pipefail
cd "$(dirname "$0")/.."

HDFS_DIR=/movielens
STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)

mkdir -p results
python3 scripts/generate_sample_data.py data   # skip if you copied real MovieLens

echo "== Upload to HDFS =="
hdfs dfs -mkdir -p $HDFS_DIR
hdfs dfs -put -f data/movies.csv data/ratings.csv $HDFS_DIR/

echo "== Hadoop MapReduce (streaming) =="
hdfs dfs -rm -r -f $HDFS_DIR/mr_out
hadoop jar "$STREAMING_JAR" \
  -files mapreduce/mapper.py,mapreduce/reducer.py,data/movies.csv \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input $HDFS_DIR/ratings.csv -output $HDFS_DIR/mr_out
hdfs dfs -cat $HDFS_DIR/mr_out/part-* > results/mapreduce_results.tsv

echo "== Spark on YARN =="
spark-submit --master yarn --deploy-mode client spark/genre_avg.py \
  hdfs://$HDFS_DIR/movies.csv hdfs://$HDFS_DIR/ratings.csv results/ 2>/dev/null || \
spark-submit --master yarn --deploy-mode client spark/genre_avg.py \
  $HDFS_DIR/movies.csv $HDFS_DIR/ratings.csv results/

echo "Done. Start the UI:  python3 app/app.py   (http://<master-ip>:5000)"
