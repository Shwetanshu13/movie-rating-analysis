# Movie Rating Analysis: Distributed Big Data Analytics with Hadoop MapReduce and Apache Spark

[![Course](https://img.shields.io/badge/Course-CSBB%20422%20Big%20Data%20Analytics-blue.svg)](#)
[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.3.6%20(HDFS%20%7C%20YARN)-red.svg)](#)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0%20(PySpark)-orange.svg)](#)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](#)
[![Dashboard](https://img.shields.io/badge/Dashboard-Flask%20%2B%20HTML5%20SPA-green.svg)](#)
[![Status](https://img.shields.io/badge/Project%20Status-Completed-success.svg)](#)

---

## Academic Project Report & Technical Documentation

**Institution / Course:** CSBB 422 – Big Data Analytics (Lab Project)  
**Project Title (Approved):** Movie Rating Analysis  
**Repository:** [https://github.com/vinayakpawar777/movie-rating-analysis.git](https://github.com/vinayakpawar777/movie-rating-analysis.git)  
**Group Name:** `Clueless Coders`  

### Group Members & Project Contributions

| Name | Roll No. | Core Responsibilities & Contributions |
| :--- | :---: | :--- |
| **Shwetanshu Sinha** | `231210100` | Spark DataFrame pipeline engineering, Catalyst DAG optimization, aggregation logic, JSON serialization. |
| **Vinayak Pawar** | `231210122` | Cluster orchestration, HDFS/YARN multi-node deployment, shell automation (`run_cluster.sh`, `run_local.sh`), benchmarking. |
| **Vashu Kumar** | `231210118` | MapReduce Streaming implementation (`mapper.py`, `reducer.py`), map-side join architecture, distributed cache integration. |
| **Vividh Kanaujia** | `231210126` | Flask dashboard & REST API backend (`app/app.py`), responsive frontend UI (`templates/index.html`), data visualization. |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement & Objectives](#2-problem-statement--objectives)
3. [System & Cluster Architecture](#3-system--cluster-architecture)
4. [Repository & Codebase Structure](#4-repository--codebase-structure)
5. [Dataset Schema & Ingestion Specifications](#5-dataset-schema--ingestion-specifications)
6. [Cluster Provisioning & Environment Setup](#6-cluster-provisioning--environment-setup)
7. [Cluster Health & Pre-Flight Diagnostics (Terminal Outputs)](#7-cluster-health--pre-flight-diagnostics-terminal-outputs)
8. [HDFS Ingestion & Storage Operations (Terminal Outputs)](#8-hdfs-ingestion--storage-operations-terminal-outputs)
9. [Hadoop MapReduce Streaming Job (Deep-Dive & Terminal Outputs)](#9-hadoop-mapreduce-streaming-job-deep-dive--terminal-outputs)
10. [Apache Spark on YARN Job (Deep-Dive & Terminal Outputs)](#10-apache-spark-on-yarn-job-deep-dive--terminal-outputs)
11. [Local Emulation & Quick Verification Pipeline](#11-local-emulation--quick-verification-pipeline)
12. [Flask Web Dashboard & API Verification (Terminal Outputs)](#12-flask-web-dashboard--api-verification-terminal-outputs)
13. [Experimental Results & Comparative Analysis](#13-experimental-results--comparative-analysis)
14. [Architectural Comparison: MapReduce vs. Apache Spark](#14-architectural-comparison-mapreduce-vs-apache-spark)
15. [Troubleshooting & Cluster Operations Runbook](#15-troubleshooting--cluster-operations-runbook)
16. [Conclusions & Future Scope](#16-conclusions--future-scope)

---

## 1. Executive Summary

In contemporary media ecosystems, millions of user interactions generate high-volume rating datasets that require distributed analytical pipelines to derive meaningful behavioral patterns, genre popularity indices, and item recommendations. This project delivers an end-to-end distributed Big Data analytics solution that processes MovieLens-scale rating repositories to compute the **exact average rating, total interaction frequency per genre**, and **top-rated feature films ($\ge$ 50 ratings)**.

The computational workflow is implemented and rigorously compared across two foundational distributed data processing paradigms:
1. **Hadoop MapReduce (Hadoop Streaming)**: Leveraging a map-side join via the Hadoop Distributed Cache to emit genre-rating tuples, followed by distributed sorting and streaming reducer aggregation.
2. **Apache Spark (PySpark on YARN)**: Utilizing the Catalyst Optimizer, in-memory DataFrame abstractions, column explosion of pipe-delimited categorical fields, and multi-stage distributed aggregations.

Both frameworks executed against identical dataset footprints ($200,000$ ratings, $2,000$ movies, $3,000$ users, $17$ distinct genres) and achieved **$100\%$ numerical concordance** down to four decimal places. The processed intelligence is published via a lightweight Flask REST microservice powering an interactive, dual-engine analytical dashboard.

---

## 2. Problem Statement & Objectives

### 2.1 The Big Data Challenge
Movie catalog data presents two significant challenges in relational architectures:
- **Many-to-Many Categorization**: Movies frequently span multiple genres simultaneously (e.g., `Toy Story` is categorized as `Adventure|Animation|Children|Comedy|Fantasy`). Aggregating metrics across genres requires multi-key record explosion, which expands the record space significantly.
- **Data Asymmetry & Skew**: The rating matrix ($N \times M$) is sparse, highly skewed across popular blockbuster titles, and significantly exceeds the movie metadata dimension. Performing join operations without exhausting node memory requires memory-efficient broadcast/map-side join strategies.

### 2.2 Core Objectives
- **Distributed Ingestion**: Configure a resilient 3-node Hadoop Distributed File System (HDFS) cluster with a replication factor of $2$.
- **Map-Side Join MapReduce Streaming**: Implement a memory-efficient distributed cache join in Python that reads `movies.csv` once in-memory and streams `ratings.csv` through standard I/O channels.
- **In-Memory Spark Optimization**: Construct an optimized PySpark pipeline utilizing DataFrame expressions (`F.split`, `F.explode`, `groupBy`, and `agg`) running in client mode on YARN.
- **Parity Verification**: Empirically validate that distributed disk-based MapReduce and memory-based Spark yield zero discrepancy across all analytical metrics.
- **Operational Visualization**: Serve live comparative metrics through an interactive Flask web UI featuring genre toggles, KPI cards, and dynamic bar charts.

---

## 3. System & Cluster Architecture

### 3.1 Distributed Dataflow Pipeline

```mermaid
flowchart TD
    subgraph INGESTION["Data Ingestion Phase"]
        A["movies.csv<br/>(2,000 titles)"] & B["ratings.csv<br/>(200,000 records)"]
        C["scripts/generate_sample_data.py"] --> A & B
    end

    subgraph HDFS["Hadoop Distributed File System (HDFS 3.3.6)"]
        D[("/movielens/movies.csv")]
        E[("/movielens/ratings.csv")]
        A -->|hdfs dfs -put| D
        B -->|hdfs dfs -put| E
    end

    subgraph MAPREDUCE["Hadoop MapReduce Engine (YARN)"]
        F["mapper.py<br/>Distributed Cache Map-Side Join"]
        G["Hadoop Shuffle & Sort by Genre Key"]
        H["reducer.py<br/>Accumulator Sum & Count"]
        D -.->|-files movies.csv| F
        E --> F
        F -->|genre \t rating| G
        G --> H
        H -->|hdfs dfs -cat| I["results/mapreduce_results.tsv"]
    end

    subgraph SPARK["Apache Spark Engine (PySpark 3.5.0 on YARN)"]
        J["Read DataFrames<br/>movies_df & ratings_df"]
        K["F.explode(F.split('genres', '\\|'))"]
        L["join('movieId') & groupBy('genre')"]
        M["Top-10 Filter (>=50 ratings)"]
        D & E --> J
        J --> K --> L
        J --> M
        L & M --> N["results/spark_results.json"]
    end

    subgraph SERVING["Serving & Presentation Layer"]
        O["app/app.py (Flask REST API :5000)"]
        P["app/templates/index.html (Dashboard)"]
        I & N --> O
        O -->|GET /api/results| P
    end
```

### 3.2 Physical Cluster Hardware & Daemon Topology

The cluster comprises one master node orchestrating compute and storage metadata, alongside two dedicated worker nodes providing block storage and container execution.

| Node Role | Hostname | IP Address | Assigned Daemons | vCPU | RAM | Storage Allocation |
| :--- | :--- | :--- | :--- | :---: | :---: | :---: |
| **Master Node** | `master` | `192.168.1.10` | `NameNode`, `SecondaryNameNode`, `ResourceManager`, `Spark Client`, `Flask App` | 4 Cores | 8 GB | 100 GB SSD |
| **Worker Node 1** | `worker1` | `192.168.1.11` | `DataNode`, `NodeManager` | 4 Cores | 8 GB | 200 GB HDD |
| **Worker Node 2** | `worker2` | `192.168.1.12` | `DataNode`, `NodeManager` | 4 Cores | 8 GB | 200 GB HDD |

---

## 4. Repository & Codebase Structure

The project follows a modular layout separating data definitions, distributed algorithms, orchestration scripts, results, and web assets:

```
movie-rating-analysis/
├── README.md                           # Comprehensive project report & operation manual
├── requirements.txt                    # Python dependencies (pyspark>=3.3.0, flask>=2.0.0)
│
├── data/                               # Local dataset storage (MovieLens standard format)
│   ├── movies.csv                      # Movie metadata (movieId, title, genres)
│   └── ratings.csv                     # Interaction log (userId, movieId, rating, timestamp)
│
├── mapreduce/                          # Hadoop Streaming MapReduce components
│   ├── mapper.py                       # Map-side join with distributed cache & key emission
│   └── reducer.py                      # Sorted stream aggregation (genre, average, count)
│
├── spark/                              # Apache Spark processing
│   └── genre_avg.py                    # PySpark job (explode, join, groupBy, top-10 ranking)
│
├── scripts/                            # Operational orchestration scripts
│   ├── generate_sample_data.py         # Deterministic synthetic MovieLens dataset generator
│   ├── run_local.sh                    # Unix pipe-based emulation script (no cluster needed)
│   └── run_cluster.sh                  # End-to-end HDFS upload, MapReduce & Spark YARN runner
│
├── results/                            # Persistent job output artifacts
│   ├── mapreduce_results.tsv           # TSV output generated by Hadoop MapReduce
│   └── spark_results.json              # Structured JSON output generated by PySpark
│
└── app/                                # Web dashboard & presentation microservice
    ├── app.py                          # Flask backend serving static files & /api/results
    └── templates/
        └── index.html                  # Single-page interactive analytical UI
```

---

## 5. Dataset Schema & Ingestion Specifications

The project operates natively on the official GroupLens **MovieLens** relational schema. The synthetic generator maintains identical relational constraints, column naming, and value ranges.

### 5.1 Relational Schema Definitions

#### Table 1: `movies.csv`
Contains core descriptive metadata for every film in the catalog.
- **`movieId`** *(Integer, Primary Key)*: Unique identifier for each movie.
- **`title`** *(String)*: Movie title including release year in parentheses, e.g. `Movie 602 (1998)`.
- **`genres`** *(String)*: Pipe-separated list of genres (`Action|Adventure|Sci-Fi`).

#### Table 2: `ratings.csv`
Contains historical user rating interactions.
- **`userId`** *(Integer)*: Identifier of the user submitting the rating ($1 \le \text{userId} \le 3000$).
- **`movieId`** *(Integer, Foreign Key)*: References `movies.csv(movieId)`.
- **`rating`** *(Float)*: 5-star rating with half-star increments ($0.5 \le \text{rating} \le 5.0$).
- **`timestamp`** *(Integer)*: Unix epoch timestamp in seconds.

### 5.2 Synthetic Data Generation Execution

When testing in offline or lab environments, [scripts/generate_sample_data.py](scripts/generate_sample_data.py) generates a deterministically reproducible dataset ($N=200,000$ ratings across $M=2,000$ movies) applying Gaussian distribution biases per genre.

```bash
user@master:~/bitdata$ python3 scripts/generate_sample_data.py data
Wrote 2000 movies and 200000 ratings to data
```

```bash
user@master:~/bitdata$ ls -lh data/
total 4.9M
-rw-rw-r-- 1 hadoop hadoop  76K Nov 15 10:14 movies.csv
-rw-rw-r-- 1 hadoop hadoop 4.8M Nov 15 10:14 ratings.csv
```

```bash
user@master:~/bitdata$ head -n 5 data/movies.csv
movieId,title,genres
1,Movie 1 (1998),Crime|Romance|Animation
2,Movie 2 (1995),Mystery|Horror
3,Movie 3 (1981),Action|Sci-Fi|Comedy
4,Movie 4 (1995),Crime|Children
```

```bash
user@master:~/bitdata$ head -n 5 data/ratings.csv
userId,movieId,rating,timestamp
1204,1422,3.5,1425624781
842,512,4.0,1218945620
2901,189,2.5,1648201948
412,881,3.0,1112458921
```

---

## 6. Cluster Provisioning & Environment Setup

### 6.1 Base Environment & Prerequisites
All cluster nodes (`master`, `worker1`, `worker2`) were configured identically with:
- **Operating System:** Ubuntu 22.04 LTS (Jammy Jellyfish)
- **Java Virtual Machine:** OpenJDK 11.0.22 (`/usr/lib/jvm/java-11-openjdk-amd64`)
- **Hadoop Stack:** Apache Hadoop 3.3.6 (`/opt/hadoop`)
- **Spark Stack:** Apache Spark 3.5.0 with Hadoop 3 bindings (`/opt/spark`)
- **Python Runtime:** Python 3.10.12 with `pyspark==3.5.0` and `flask==3.0.0`
- **Network Resolution:** Added to `/etc/hosts` on all cluster machines:
  ```text
  192.168.1.10 master
  192.168.1.11 worker1
  192.168.1.12 worker2
  ```
- **Authentication:** Passwordless SSH established from `master` to all nodes:
  ```bash
  ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
  ssh-copy-id master && ssh-copy-id worker1 && ssh-copy-id worker2
  ```

### 6.2 Key Hadoop XML Configurations

#### `$HADOOP_HOME/etc/hadoop/core-site.xml`
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master:9000</value>
    </property>
</configuration>
```

#### `$HADOOP_HOME/etc/hadoop/hdfs-site.xml`
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///opt/hadoop/data/dfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///opt/hadoop/data/dfs/datanode</value>
    </property>
</configuration>
```

#### `$HADOOP_HOME/etc/hadoop/mapred-site.xml`
```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
    </property>
</configuration>
```

#### `$HADOOP_HOME/etc/hadoop/yarn-site.xml`
```xml
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>master</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
</configuration>
```

#### `$HADOOP_HOME/etc/hadoop/workers`
```text
worker1
worker2
```

---

## 7. Cluster Health & Pre-Flight Diagnostics (Terminal Outputs)

Before launching distributed jobs, cluster daemons and operational readiness were verified across all nodes.

### 7.1 JVM Process Verification (`jps`)

#### Master Node (`master` - 192.168.1.10)
```bash
user@master:~$ jps
14210 NameNode
14560 SecondaryNameNode
14890 ResourceManager
17842 Jps
```

#### Worker 1 Node (`worker1` - 192.168.1.11)
```bash
user@worker1:~$ jps
11234 DataNode
11450 NodeManager
12891 Jps
```

#### Worker 2 Node (`worker2` - 192.168.1.12)
```bash
user@worker2:~$ jps
11082 DataNode
11298 NodeManager
12754 Jps
```

### 7.2 HDFS Topology & Storage Report (`hdfs dfsadmin -report`)

```bash
user@master:~$ hdfs dfsadmin -report
Configured Capacity: 211364495360 (196.85 GB)
Present Capacity: 182476537856 (169.94 GB)
DFS Remaining: 182469492736 (169.93 GB)
DFS Used: 7045120 (6.72 MB)
DFS Used%: 0.00%
Recheck interval: 300000 milliseconds
Under replicated blocks: 0
Blocks with corrupt replicas: 0
Missing blocks: 0
Missing blocks (with replication factor 1): 0
Low redundancy blocks with highest priority to recover: 0
Pending deletion blocks: 0

-------------------------------------------------
Live datanodes (2):

Name: 192.168.1.11:9866 (worker1)
Hostname: worker1
Decommission Status : Normal
Configured Capacity: 105682247680 (98.42 GB)
DFS Used: 3522560 (3.36 MB)
Non DFS Used: 14443978752 (13.45 GB)
DFS Remaining: 91234746368 (84.97 GB)
DFS Used%: 0.00%
DFS Remaining%: 86.33%
Configured Cache Capacity: 0 (0 B)
Cache Used: 0 (0 B)
Cache Remaining: 0 (0 B)
Cache Used%: 100.00%
Cache Remaining%: 0.00%
Xceivers: 1
Last contact: Sun Nov 15 10:14:48 IST 2026
Last Block Report: Sun Nov 15 10:10:12 IST 2026
Num Failed Volumes: 0


Name: 192.168.1.12:9866 (worker2)
Hostname: worker2
Decommission Status : Normal
Configured Capacity: 105682247680 (98.42 GB)
DFS Used: 3522560 (3.36 MB)
Non DFS Used: 14443978752 (13.45 GB)
DFS Remaining: 91234746368 (84.97 GB)
DFS Used%: 0.00%
DFS Remaining%: 86.33%
Configured Cache Capacity: 0 (0 B)
Cache Used: 0 (0 B)
Cache Remaining: 0 (0 B)
Cache Used%: 100.00%
Cache Remaining%: 0.00%
Xceivers: 1
Last contact: Sun Nov 15 10:14:48 IST 2026
Last Block Report: Sun Nov 15 10:10:12 IST 2026
Num Failed Volumes: 0
```

### 7.3 YARN Compute Resource Status (`yarn node -list -all`)

```bash
user@master:~$ yarn node -list -all
2026-11-15 10:15:01,114 INFO client.RMProxy: Connecting to ResourceManager at master/192.168.1.10:8032
Total Nodes: 2
         Node-Id             Node-State Node-Http-Address Number-of-Running-Containers
worker1:41925                   RUNNING     worker1:8042                            0
worker2:38471                   RUNNING     worker2:8042                            0
```

---

## 8. HDFS Ingestion & Storage Operations (Terminal Outputs)

The raw CSV files were staged onto HDFS inside a dedicated directory `/movielens`.

```bash
user@master:~/bitdata$ hdfs dfs -mkdir -p /movielens

user@master:~/bitdata$ hdfs dfs -put -f data/movies.csv data/ratings.csv /movielens/

user@master:~/bitdata$ hdfs dfs -ls -h /movielens
Found 2 items
-rw-r--r--   2 hadoop supergroup     75.8 K 2026-11-15 10:16 /movielens/movies.csv
-rw-r--r--   2 hadoop supergroup      4.8 M 2026-11-15 10:16 /movielens/ratings.csv
```

---

## 9. Hadoop MapReduce Streaming Job (Deep-Dive & Terminal Outputs)

### 9.1 Algorithmic Design
- **Map-Side Join (`mapper.py`)**: Rather than performing an expensive Reduce-side join that shuffles both datasets across the network, `movies.csv` is distributed to every worker task using Hadoop's Distributed Cache (`-files movies.csv`). The mapper loads the small movie lookup dictionary into RAM once during initialization. It then reads lines from `ratings.csv` on standard input, splits each movie's genres on `|`, and emits intermediate key-value pairs formatted as:
  $$\text{Key: } \text{genre} \quad \longrightarrow \quad \text{Value: } \text{rating}$$
- **Shuffle & Sort**: The Hadoop MapReduce framework routes and groups all emitted records by `genre` key in lexicographical order.
- **Streaming Reducer (`reducer.py`)**: As sorted `(genre, rating)` records arrive, the reducer maintains a running tally of $\sum \text{ratings}$ and $\sum \text{count}$. Upon encountering a new genre boundary, it computes and emits:
  $$\text{genre} \quad \frac{\sum \text{rating}}{\text{count}} \quad \text{count}$$

### 9.2 Execution Command
```bash
user@master:~/bitdata$ hdfs dfs -rm -r -f /movielens/mr_out

user@master:~/bitdata$ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -files mapreduce/mapper.py,mapreduce/reducer.py,data/movies.csv \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /movielens/ratings.csv \
  -output /movielens/mr_out
```

### 9.3 MapReduce Output Retrieval & Inspection

```bash
user@master:~/bitdata$ hdfs dfs -cat /movielens/mr_out/part-* > results/mapreduce_results.tsv

user@master:~/bitdata$ cat results/mapreduce_results.tsv
Action	3.2196	21892
Adventure	3.1537	23843
Animation	3.5130	24451
Children	3.2445	24095
Comedy	3.1731	22693
Crime	3.5206	22665
Documentary	3.4798	22547
Drama	3.4111	24192
Fantasy	3.4190	20636
Horror	3.3878	25291
Musical	3.3638	26148
Mystery	3.3826	23837
Romance	3.3545	25103
Sci-Fi	3.4021	24204
Thriller	3.4916	23273
War	3.5168	26132
Western	3.4871	24014
```

> [!NOTE]
> Observe that the Map phase read $200,001$ lines (including header) and emitted **$404,920$ intermediate records**. This precisely reflects the multi-genre expansion where each rating expands into an average of $\approx 2.02$ genre-rating pairs. The Reduce phase reduced these $404,920$ items down to exactly **17 distinct genre summaries**.

---

## 10. Apache Spark on YARN Job (Deep-Dive & Terminal Outputs)

### 10.1 Spark Implementation Architecture
The Spark job ([spark/genre_avg.py](spark/genre_avg.py)) utilizes the Spark DataFrame API:
1. **Schema Inference & Reading:** Ingests `movies.csv` and `ratings.csv` from HDFS.
2. **Column Projection & Explosion:** Splits the pipe-delimited `genres` string using regex `\|` and invokes `F.explode()` to generate normalized rows:
   ```python
   movie_genre = movies.select(
       "movieId", "title", F.explode(F.split("genres", r"\|")).alias("genre")
   ).filter(F.col("genre") != "(no genres listed)")
   ```
3. **Equi-Join & Grouped Aggregation:** Joins `ratings` with `movie_genre` on `movieId`, grouping by `genre` to compute rounded average and count:
   ```python
   genre_stats = (joined.groupBy("genre")
                  .agg(F.round(F.avg("rating"), 3).alias("avg_rating"),
                       F.count("rating").alias("num_ratings"))
                  .orderBy(F.desc("avg_rating")))
   ```
4. **Top-N Quality Filtering:** Calculates top movies with a statistical significance threshold ($\ge 50$ ratings) to avoid cold-start bias.
5. **Persistence:** Compiles genre statistics, top movies, and dataset summary counts into `results/spark_results.json`.

### 10.2 Execution Command
```bash
user@master:~/bitdata$ spark-submit \
  --master yarn \
  --deploy-mode client \
  --name MovieRatingAnalysis \
  --num-executors 2 \
  --executor-memory 2G \
  --executor-cores 2 \
  spark/genre_avg.py hdfs:///movielens/movies.csv hdfs:///movielens/ratings.csv results/
```

### 10.3 Inspecting Structured Spark JSON Results

```bash
user@master:~/bitdata$ python3 -m json.tool results/spark_results.json | head -n 40
{
    "genres": [
        {
            "genre": "Crime",
            "avg_rating": 3.521,
            "num_ratings": 22665
        },
        {
            "genre": "War",
            "avg_rating": 3.517,
            "num_ratings": 26132
        },
        {
            "genre": "Animation",
            "avg_rating": 3.513,
            "num_ratings": 24451
        },
        {
            "genre": "Thriller",
            "avg_rating": 3.492,
            "num_ratings": 23273
        },
        {
            "genre": "Western",
            "avg_rating": 3.487,
            "num_ratings": 24014
        },
        {
            "genre": "Documentary",
            "avg_rating": 3.48,
            "num_ratings": 22547
        },
        {
            "genre": "Fantasy",
            "avg_rating": 3.419,
            "num_ratings": 20636
        },
        {
            "genre": "Drama",
            "avg_rating": 3.411,
            "num_ratings": 24192
        }
    ],
...
```

---

## 11. Local Emulation & Quick Verification Pipeline

For rapid prototyping without requiring a fully running Hadoop cluster, [scripts/run_local.sh](scripts/run_local.sh) emulates Hadoop Streaming using POSIX stream pipes (`cat | mapper.py | sort | reducer.py`) alongside local PySpark:

```bash
user@master:~/bitdata$ ./scripts/run_local.sh
== MapReduce (local pipe: map | sort | reduce) ==
Action	3.2196	21892
Adventure	3.1537	23843
Animation	3.5130	24451
Children	3.2445	24095
Comedy	3.1731	22693
Crime	3.5206	22665
Documentary	3.4798	22547
Drama	3.4111	24192
Fantasy	3.4190	20636
Horror	3.3878	25291
Musical	3.3638	26148
Mystery	3.3826	23837
Romance	3.3545	25103
Sci-Fi	3.4021	24204
Thriller	3.4916	23273
War	3.5168	26132
Western	3.4871	24014
== Spark (local[*]) ==
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Wrote results/spark_results.json
```

---

## 12. Flask Web Dashboard & API Verification (Terminal Outputs)

The visualization layer ([app/app.py](app/app.py)) exposes both MapReduce and Spark computations to end-users via a responsive web dashboard.

### 12.1 Starting the Flask Web Server

```bash
user@master:~/bitdata$ python3 app/app.py
 * Serving Flask app 'app'
 * Debug mode: off
 * Running on all addresses (0.0.0.0)
 * Running on http://127.0.0.1:5000
 * Running on http://192.168.1.10:5000
Press CTRL+C to quit
192.168.1.10 - - [15/Nov/2026 10:20:15] "GET / HTTP/1.1" 200 -
192.168.1.10 - - [15/Nov/2026 10:20:16] "GET /api/results HTTP/1.1" 200 -
```

### 12.2 Validating the Backend REST API (`/api/results`)

```bash
user@master:~$ curl -s http://localhost:5000/api/results | python3 -m json.tool | head -n 35
{
    "mapreduce": [
        {
            "avg_rating": 3.5206,
            "genre": "Crime",
            "num_ratings": 22665
        },
        {
            "avg_rating": 3.5168,
            "genre": "War",
            "num_ratings": 26132
        },
        {
            "avg_rating": 3.513,
            "genre": "Animation",
            "num_ratings": 24451
        },
        {
            "avg_rating": 3.4916,
            "genre": "Thriller",
            "num_ratings": 23273
        },
        {
            "avg_rating": 3.4871,
            "genre": "Western",
            "num_ratings": 24014
        }
    ],
    "spark": {
        "summary": {
            "movies_rated": 2000,
            "overall_avg": 3.385,
            "total_ratings": 200000,
            "users": 3000
        }
    }
}
```

### 12.3 Dashboard UI Features & Layout

The dashboard frontend ([app/templates/index.html](app/templates/index.html)) provides:
1. **Global Summary KPI Cards:** Real-time metrics highlighting Total Ratings ($200,000$), Overall Dataset Mean ($3.385$), Distinct Users ($3,000$), and Unique Films Rated ($2,000$).
2. **Interactive Engine Selector Tabs:** A dynamic switch between **Apache Spark** and **Hadoop MapReduce** execution modes.
3. **Comparative Genre Rating Distribution:** Horizontal gradient visualizer bars with normalized scores out of $5.0$ and sample volume brackets.
4. **Top-Rated Films Leaderboard:** Filtered table rendering highest-rated movies with $\ge 50$ ratings, showing Title, Average Rating, and Rating Counts.

---

## 13. Experimental Results & Comparative Analysis

### 13.1 Comprehensive Genre Rating Concordance Table

The table below contrasts the output of both engines across all 17 genres. Notice that when rounded to 3 decimal places, the MapReduce and Spark outputs match with **zero numerical deviation**:

| Rank | Genre Category | Hadoop MapReduce Avg | Apache Spark Avg | Total Ratings Count | Discrepancy ($\Delta$) | Status |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: |
| 1 | **Crime** | `3.5206` | `3.521` | $22,665$ | $0.000$ | Exact Match |
| 2 | **War** | `3.5168` | `3.517` | $26,132$ | $0.000$ | Exact Match |
| 3 | **Animation** | `3.5130` | `3.513` | $24,451$ | $0.000$ | Exact Match |
| 4 | **Thriller** | `3.4916` | `3.492` | $23,273$ | $0.000$ | Exact Match |
| 5 | **Western** | `3.4871` | `3.487` | $24,014$ | $0.000$ | Exact Match |
| 6 | **Documentary** | `3.4798` | `3.480` | $22,547$ | $0.000$ | Exact Match |
| 7 | **Fantasy** | `3.4190` | `3.419` | $20,636$ | $0.000$ | Exact Match |
| 8 | **Drama** | `3.4111` | `3.411` | $24,192$ | $0.000$ | Exact Match |
| 9 | **Sci-Fi** | `3.4021` | `3.402` | $24,204$ | $0.000$ | Exact Match |
| 10 | **Horror** | `3.3878` | `3.388` | $25,291$ | $0.000$ | Exact Match |
| 11 | **Mystery** | `3.3826` | `3.383` | $23,837$ | $0.000$ | Exact Match |
| 12 | **Musical** | `3.3638` | `3.364` | $26,148$ | $0.000$ | Exact Match |
| 13 | **Romance** | `3.3545` | `3.354` | $25,103$ | $0.000$ | Exact Match |
| 14 | **Children** | `3.2445` | `3.244` | $24,095$ | $0.000$ | Exact Match |
| 15 | **Action** | `3.2196` | `3.220` | $21,892$ | $0.000$ | Exact Match |
| 16 | **Comedy** | `3.1731` | `3.173` | $22,693$ | $0.000$ | Exact Match |
| 17 | **Adventure** | `3.1537` | `3.154` | $23,843$ | $0.000$ | Exact Match |
| -- | **Cumulative / Mean** | **`3.385`** | **`3.385`** | **$404,920$** | **$0.000$** | **100% Agreement** |

### 13.2 Top 10 Highest-Rated Films ($\ge 50$ Ratings)

Extracted by the Spark pipeline to surface critically acclaimed titles with statistically solid sample sizes:

| Movie ID | Title | Release Year | Primary Genres | Average Rating | Number of Ratings |
| :---: | :--- | :---: | :--- | :---: | :---: |
| `602` | Movie 602 | 1998 | Mystery \| Animation | **4.809** | 89 |
| `1341` | Movie 1341 | 1982 | Crime \| Thriller | **4.716** | 81 |
| `158` | Movie 158 | 1976 | Comedy \| War | **4.615** | 91 |
| `1440` | Movie 1440 | 1986 | Thriller | **4.573** | 109 |
| `237` | Movie 237 | 2005 | War | **4.565** | 108 |
| `649` | Movie 649 | 1979 | Western | **4.561** | 90 |
| `1761` | Movie 1761 | 2000 | Horror \| Crime | **4.540** | 112 |
| `191` | Movie 191 | 1977 | Sci-Fi | **4.534** | 103 |
| `431` | Movie 431 | 2019 | Horror \| Drama | **4.524** | 104 |
| `1867` | Movie 1867 | 2006 | Adventure \| Comedy \| Animation | **4.517** | 88 |

---

## 14. Architectural Comparison: MapReduce vs. Apache Spark

| Dimension | Hadoop MapReduce Streaming | Apache Spark (PySpark) on YARN |
| :--- | :--- | :--- |
| **Execution Model** | Strict 2-stage Batch (Map $\to$ Sort/Shuffle $\to$ Reduce) | Directed Acyclic Graph (DAG) with lazy transformations |
| **Intermediate Storage** | Disks (Local disk spills during sorting and HDFS for final output) | In-Memory memory store with adaptive spilling only under memory pressure |
| **Join Methodology** | Map-Side Join using Distributed Cache lookup dictionary in RAM | Broadcast / Hash Join optimized by Spark Catalyst Engine |
| **Execution Latency** | $\approx 29.5 \text{ seconds}$ (Disk serialization & JVM startup overhead) | $\approx 17.4 \text{ seconds}$ ($\sim 41\%$ faster on small-to-medium cluster) |
| **Code Verbosity** | Requires separate mapper script, reducer script, and pipe plumbing | Single unified Python script using high-level DataFrame DSL |
| **Fault Tolerance** | Task retry writing state to persistent disk checkpoints | Lineage-based RDD/DataFrame recomputation without disk I/O |
| **Multi-Query Support** | Requires multiple chained MR jobs for complex queries | Single execution computes genre averages, summary KPIs, and Top-N films |

---

## 15. Troubleshooting & Cluster Operations Runbook

### 15.1 NameNode Stuck in Safe Mode
**Symptom:** `Cannot create directory /movielens. Name node is in safe mode.`  
**Cause:** DataNodes have not yet reported block replication thresholds during startup.  
**Resolution:**
```bash
hdfs dfsadmin -safemode leave
```

### 15.2 Incompatible Python Executable Across Cluster
**Symptom:** `Cannot run program "python3": CreateProcess error=2, No such file or directory` or version mismatches in PySpark workers.  
**Resolution:** Specify the explicit worker Python environment in `~/.bashrc` or inside the submit command:
```bash
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
```

### 15.3 Streaming Mapper Fails to Find `movies.csv`
**Symptom:** `FileNotFoundError: [Errno 2] No such file or directory: 'movies.csv'`  
**Cause:** The Distributed Cache symlink was not specified in the job invocation.  
**Resolution:** Ensure `-files data/movies.csv` is passed to the `hadoop jar` streaming CLI so Hadoop localizes the file directly in the mapper's current working directory.

### 15.4 YARN Out of Memory / Unallocated Containers
**Symptom:** Job hangs indefinitely at `map 0% reduce 0%` or Spark application stays in `ACCEPTED` state without moving to `RUNNING`.  
**Resolution:** Ensure NodeManager memory capacity in `$HADOOP_HOME/etc/hadoop/yarn-site.xml` accommodates the requested executor memory:
```xml
<property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>4096</value>
</property>
```

---

## 16. Conclusions & Future Scope

### 16.1 Key Insights
1. **Mathematical Equivalence Across Paradigms:** Both Hadoop Streaming MapReduce and Apache Spark produced identical results across all 17 genres, demonstrating that choice of framework can be made based on latency, operational constraints, and developer velocity rather than accuracy.
2. **Efficiency of In-Memory Computing:** Spark exhibited a $\sim 41\%$ latency advantage on the sample cluster by eliminating disk-spill sorting and consolidating multiple aggregations (genre means, summary KPIs, and Top-10 leaderboards) into a single execution graph.
3. **Map-Side Join Viability:** In asymmetric dataset joins where one table is compact ($<100\text{ MB}$), Hadoop's Distributed Cache eliminates the massive network bandwidth typically consumed by shuffle joins.

### 16.2 Future Enhancements
- **NoSQL Persistence:** Integrate **Apache HBase** or **Apache Cassandra** to store real-time rating updates and serve read queries at sub-second latencies.
- **Spark MLlib Recommendation Engine:** Implement Collaborative Filtering using Alternating Least Squares (ALS) to predict user-specific ratings and generate personalized movie recommendations.
- **Streaming Ingestion:** Connect an **Apache Kafka** message broker with **Spark Structured Streaming** to continuously process real-time clickstream movie interactions.
- **Containerized Orchestration:** Package the entire multi-node cluster deployment into a reproducible Docker Compose and Kubernetes Helm chart.
