# Movie Rating Analysis (Hadoop + Spark)

**Course:** CSBB 422 – Big Data Analytics (Lab Project)
**Project title (as approved):** Movie Rating Analysis
**Group:** `Clueless Coders`

| Name | Roll No. |
|------|----------|
| `Shwetanshu Sinha` | `231210100` |
| `Vinayak Pawar` | `231210122` |
| `Vashu Kumar` | `231210118` |
| `Vividh Kanaujia` | `231210126` |

## 1. Objective
Analyze a MovieLens-style ratings dataset and compute the **average rating per genre** (plus rating counts and the top-rated movies) on a Hadoop cluster, using both **Hadoop MapReduce (Streaming)** and **Apache Spark**, and present the results in a web dashboard.

## 2. Architecture

```
 movies.csv, ratings.csv
          │  hdfs dfs -put
          ▼
   ┌─────────────── HDFS (replicated over workers) ───────────────┐
   │                                                               │
   ├─► Hadoop MapReduce (YARN)  mapper.py → shuffle/sort → reducer.py ─► results/mapreduce_results.tsv
   │                                                               │
   └─► Spark on YARN  genre_avg.py (explode genres, join, groupBy) ─► results/spark_results.json
                                                                   │
                                          Flask UI (app/app.py)  ◄─┘  http://<master>:5000
```

**Cluster layout** (fill in your actual hostnames/IPs):

| Role | Host | Daemons |
|------|------|---------|
| Master | `master` (`<IP>`) | NameNode, ResourceManager, (SecondaryNameNode), Spark client, Flask UI |
| Worker 1 | `worker1` (`<IP>`) | DataNode, NodeManager |
| Worker 2 | `worker2` (`<IP>`) | DataNode, NodeManager |

## 3. Technology
Hadoop 3.x (HDFS, YARN, MapReduce Streaming), Apache Spark (PySpark), Python 3, Flask, plain HTML/CSS/JS front end.

## 4. Repository layout
```
data/                 movies.csv, ratings.csv (MovieLens schema)
mapreduce/            mapper.py, reducer.py (Hadoop Streaming)
spark/genre_avg.py    PySpark job
app/                  Flask dashboard (app.py, templates/index.html)
scripts/              generate_sample_data.py, run_local.sh, run_cluster.sh
results/              job outputs read by the UI
requirements.txt
```

## 5. Dataset
`scripts/generate_sample_data.py` generates a **synthetic dataset in the exact MovieLens schema** (2,000 movies, 3,000 users, 200,000 ratings) so the project runs anywhere. To use real data, download MovieLens (https://grouplens.org/datasets/movielens/, e.g. `ml-latest-small`) and copy its `movies.csv` and `ratings.csv` into `data/`; no code changes are needed.

- `movies.csv`: `movieId,title,genres` (genres separated by `|`)
- `ratings.csv`: `userId,movieId,rating,timestamp`

## 6. Installation and configuration (all nodes unless noted)

1. **Prerequisites:** Ubuntu, OpenJDK 11 (`sudo apt install openjdk-11-jdk`), Python 3, `ssh`.
2. **Hostnames:** add to `/etc/hosts` on every node: `<IP> master`, `<IP> worker1`, `<IP> worker2`.
3. **Passwordless SSH from master:** `ssh-keygen -t rsa`, then `ssh-copy-id` to `master`, `worker1`, `worker2`.
4. **Hadoop:** download and extract Hadoop 3.x to `/opt/hadoop`, set in `~/.bashrc`:
   `JAVA_HOME`, `HADOOP_HOME=/opt/hadoop`, `PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin`.
5. **Config files** in `$HADOOP_HOME/etc/hadoop/`:
   - `core-site.xml`: `fs.defaultFS = hdfs://master:9000`
   - `hdfs-site.xml`: `dfs.replication = 2`, NameNode/DataNode directories
   - `mapred-site.xml`: `mapreduce.framework.name = yarn`
   - `yarn-site.xml`: `yarn.resourcemanager.hostname = master`, `yarn.nodemanager.aux-services = mapreduce_shuffle`
   - `workers`: list `worker1` and `worker2`
6. **Spark:** extract Spark 3.x/4.x to `/opt/spark`, set `SPARK_HOME`, `HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop`; `pip install pyspark flask` on the master.
7. **Start the cluster (master):**
   ```bash
   hdfs namenode -format        # first time only
   start-dfs.sh && start-yarn.sh
   jps                          # master: NameNode, ResourceManager; workers: DataNode, NodeManager
   hdfs dfsadmin -report        # should show 2 live datanodes
   ```
   Web UIs: NameNode `http://master:9870`, YARN `http://master:8088`.

## 7. Basic commands used
| Purpose | Command |
|---------|---------|
| Make HDFS dir | `hdfs dfs -mkdir -p /movielens` |
| Upload | `hdfs dfs -put -f data/*.csv /movielens/` |
| List / read | `hdfs dfs -ls /movielens`, `hdfs dfs -cat /movielens/mr_out/part-*` |
| Remove output | `hdfs dfs -rm -r -f /movielens/mr_out` |
| Run MapReduce | `hadoop jar hadoop-streaming-*.jar -files ... -mapper ... -reducer ...` |
| Run Spark | `spark-submit --master yarn spark/genre_avg.py <movies> <ratings> results/` |
| Cluster status | `jps`, `hdfs dfsadmin -report`, `yarn node -list` |

## 8. How it works

**MapReduce (Hadoop Streaming).** `mapper.py` loads the small `movies.csv` from the distributed cache (map-side join), reads each rating line and emits one `genre<TAB>rating` pair per genre of the movie. Hadoop shuffles and sorts by genre; `reducer.py` sums ratings and counts per genre and emits `genre, average, count`.

**Spark.** `genre_avg.py` reads both CSVs as DataFrames, explodes the pipe-separated `genres` column into one row per (movie, genre), joins with ratings, and uses `groupBy("genre").agg(avg, count)`. It also computes the top 10 movies (≥ 50 ratings) and dataset summary statistics, then writes `results/spark_results.json`.

**UI.** `app/app.py` (Flask) serves `/api/results` (both outputs as JSON) and a dashboard (`templates/index.html`) with summary cards, a per-genre bar chart (switch between Spark and MapReduce results) and a top-movies table.

## 9. How to run

**Quick local test (no cluster):**
```bash
pip install -r requirements.txt
./scripts/run_local.sh
python3 app/app.py            # open http://localhost:5000
```

**On the cluster (master node):**
```bash
./scripts/run_cluster.sh
python3 app/app.py            # open http://<master-ip>:5000
```

## 10. Results
Local run on the 200,000-rating sample (Spark and MapReduce outputs were checked and agree exactly for all 17 genres):

| Genre | Avg rating | # ratings |
|-------|-----------|-----------|
| Crime | 3.52 | 22,665 |
| War | 3.52 | 26,132 |
| Animation | 3.51 | 24,451 |
| Thriller | 3.49 | 23,273 |
| Western | 3.49 | 24,014 |
| ... | ... | ... |
| Adventure | 3.15 | 23,843 |

Overall average: 3.385. Full per-genre output is in `results/`. Note that the sample data is synthetic, so genre rankings carry no real-world meaning; rerun with real MovieLens data for genuine findings.

**Screenshots** (add before submitting): `jps` on each node, HDFS UI (2 live DataNodes), YARN UI with the finished jobs, and the dashboard.

## 11. Conclusion
Both frameworks produced identical per-genre averages. MapReduce gives a simple, disk-based baseline, while Spark expresses the same job in a few DataFrame operations and runs faster thanks to in-memory processing. The pipeline scales by adding worker nodes and larger datasets.

## 12. Future work
Add HBase storage for results, per-year trends, and a collaborative-filtering recommender with Spark MLlib (ALS).
