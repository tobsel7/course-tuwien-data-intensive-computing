#!/bin/bash

STOPWORDS="data/stopwords.txt"
INPUT="data/reviews_devset.json"
COUNTS="result/local_review_counts.json"
OUTPUT="result/local_output.txt"

echo "[STEP 1] Running MapReduce preprocessing..."
python3 src/amazon-review-counter.py --stopwords "$STOPWORDS" < "$INPUT" > "$COUNTS"

echo "[STEP 2] Calculating chi-square scores..."
python3 src/chi-square.py "$COUNTS" > "$OUTPUT"

echo "Done. Final result: $OUTPUT"