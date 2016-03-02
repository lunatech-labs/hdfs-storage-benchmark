#!/usr/bin/env bash

# What do I do?
###############

# Run the benchmark on a Yarn Cluster with HDFS, so fingers crossed...

# CONFIGURATION SECTION
#######################

# Set the global configuration

# The location of the fat-jar for the application (run `sbt assembly` to create it)
export JAR_FILE="/tmp/hdfs-storage-benchmark-fat.jar"
# The location of the fat-jar for the application (run `sbt assembly` to create it)
export MAIN_CLASS="lunatech.benchmarks.hdfs.Benchmark"

export SPARK_MASTER_URL=yarn-cluster

export YARN_EXECUTORS=3
#
export SPARK_DRIVER_MEMORY=5g
#
export SPARK_EXECUTOR_MEMORY=3g

# Where are all the necessary files? This path should be accessible by all spark nodes
# !!! Make sure all nodes have access to it
export ROOT_DIR="hdfs:///tmp/"
# The temporary HDFS directory were the data will be persisted as avro/parquet/csv...
# The data is cleaned after each step so it should not overflow
export APP_PATH_TMP="$ROOT_DIR"
# THe path to the markdown file used to store the results
export APP_RESULTS_FILE="$ROOT_DIR/hdfs-storage-benchmark-results.md"
# How many entries from sample file should be used for benchmarking?
# If the specified size is larger than the number of records in the sample file the data will be padded to match the given number.
export APP_START_SIZE=200000
# How fast should the data increase (1 means that the test data will increase with the same amount at each step)
export APP_INCREMENT_SIZE=200000
# How many steps should we take to provide good results
export APP_INCREMENT_STEPS=10
# How many times should we run the benchmark for each step (useful to get some averages)
export APP_RUNS_PER_STEP=3
# Do we want the results of each individual run per step or the averages of the 'runs.per.step'?
export APP_COLLECT_AVERAGES=true


# RUN SECTION
#############

spark-submit \
  --class $MAIN_CLASS \
  --name "HDFS-Storage-Benchmark" \
  --master $SPARK_MASTER_URL \
  --num-executors $YARN_EXECUTORS \
  --conf spark.task.maxFailures=20 \
  --conf spark.driver.memory=$SPARK_DRIVER_MEMORY \
  --conf spark.executor.memory=$SPARK_EXECUTOR_MEMORY \
  --verbose \
  $JAR_FILE \
  serial-killer.path.tmp=\"$APP_PATH_TMP\" \
  serial-killer.results.file=\"$APP_RESULTS_FILE\" \
  serial-killer.start.size=$APP_START_SIZE \
  serial-killer.increment.size=$APP_INCREMENT_SIZE \
  serial-killer.increment.steps=$APP_INCREMENT_STEPS \
  serial-killer.runs.per.step=$APP_RUNS_PER_STEP \
  serial-killer.collect.averages=$APP_COLLECT_AVERAGES
