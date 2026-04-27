#!/bin/bash

# Runs the complete pipeline on the full Amazon reviews dataset.
#
# Step 1:
#   amazon-review-counter.py counts document frequencies for each
#   <term, category> pair and the total number of reviews per category.
#
# Step 2:
#   MRj-chi-square.py calculates Chi-Square scores from these counts and
#   selects the top 75 terms per category.
#
# Step 3:
#   The final Hadoop output is merged into:
#       result/full_output.txt
#
# The script is stored in src/, but it automatically switches to the project
# root directory before running the jobs.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

STREAMING_JAR="/usr/lib/hadoop/tools/lib/hadoop-streaming.jar"
STOPWORDS="data/stopwords.txt"
INPUT="hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json"

COUNTS_DIR="hdfs:///user/$(whoami)/result/full_review_counts"
CHI_DIR="hdfs:///user/$(whoami)/result/full_chi_square"

OUTPUT="result/full_output.txt"

rm -f "$OUTPUT"
mkdir -p result

hadoop fs -rm -r -f "$COUNTS_DIR" "$CHI_DIR"

echo "[Step 1] Running review counter..."
python3 src/amazon-review-counter.py -r hadoop \
    --hadoop-streaming-jar "$STREAMING_JAR" \
    --stopwords "$STOPWORDS" \
    --output-dir "$COUNTS_DIR" \
    "$INPUT"

echo "[Step 2] Running chi-square..."
python3 src/MRj-chi-square.py -r hadoop \
    --hadoop-streaming-jar "$STREAMING_JAR" \
    --output-dir "$CHI_DIR" \
    "$COUNTS_DIR"

echo "[Step 3] Copying result..."
hadoop fs -getmerge "$CHI_DIR" "$OUTPUT"

echo "Done. Final result: $OUTPUT"