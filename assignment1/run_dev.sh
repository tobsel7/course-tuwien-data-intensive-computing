#!/bin/bash

STREAMING_JAR="/usr/lib/hadoop/tools/lib/hadoop-streaming.jar"
STOPWORDS="data/stopwords.txt"
INPUT="hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"
TMP_DIR="hdfs:///user/$(whoami)/result/dev_review_counts"
COUNTS="result/dev_review_counts.json"
OUTPUT="result/dev_output.txt"

echo "[STEP 1] Running MapReduce on Hadoop..."
python3 src/amazon-review-counter.py -r hadoop \
    --hadoop-streaming-jar "$STREAMING_JAR" \
    --stopwords "$STOPWORDS" \
    --output-dir "$TMP_DIR" \
    "$INPUT"

echo "[STEP 2] Copy HDFS parts to local disk..."
hadoop fs -getmerge "$TMP_DIR" "$COUNTS"

echo "[STEP 3] Calculating Chi-Square ..."
python3 src/chi-square.py "$COUNTS" > "$OUTPUT"

echo "Done. Final result: $OUTPUT"

echo "Cleaning up temporary HDFS results..."
hadoop fs -rm -r "$TMP_DIR" 2>/dev/null