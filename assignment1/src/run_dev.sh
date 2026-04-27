#!/bin/bash

# Runs the complete MR pipeline on the Amazon reviews dev dataset.
#
# The script can be executed from any directory. It first detects the project
# root directory, then runs both MapReduce jobs from there.
#
# Step 1:
#   amazon-review-counter.py counts document frequencies for each
#   <term, category> pair and also counts the total number of reviews per
#   category.
#
# Step 2:
#   MRj-chi-square.py reads the counts from Step 1 and calculates Chi-Square
#   scores. It selects the top 75 terms per category and creates the final
#   merged dictionary.
#
# Step 3:
#   The final Hadoop output is merged from HDFS into:
#       result/dev_output.txt

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

STREAMING_JAR="/usr/lib/hadoop/tools/lib/hadoop-streaming.jar"
STOPWORDS="data/stopwords.txt"
INPUT="hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"

COUNTS_DIR="hdfs:///user/$(whoami)/result/dev_review_counts"
CHI_DIR="hdfs:///user/$(whoami)/result/dev_chi_square"

OUTPUT="result/dev_output.txt"

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
