from mrjob.job import MRJob
import json
import re

class AmazonReviewCounter(MRJob):
    """
    MapReduce job for counting document frequencies of terms per product category.

    Counts how often each word appears in reviews of each category.

    Each word is counted at most once per review. This gives us document
    frequencies per category, which are later used for the Chi-Square calculation.

    Input:
        <offset, review_json>

    Mapper output:
        <(term, category), 1>
        <("__TOTAL__", category), 1>

    Combiner:
        <key, [1, 1, ...]> -> <key, local_sum>

    Partition / Sort / Shuffle:
        Hadoop sends equal keys to the same reducer and groups their values.

    Reducer output:
        <(term, category), document_frequency>
        <("__TOTAL__", category), category_total>
    """

    def configure_args(self):
        """Adds the stopwords file as command-line argument: --stopwords."""

        super(AmazonReviewCounter, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords.txt")

    def mapper_init(self):
        """Loads stopwords once per mapper and defines the token delimiter pattern."""

        # Load stopwords into a set
        with open(self.options.stopwords, "r") as file:
            self.stopwords = set(file.read().split())
        self.delimiters = r"[\s\d\(\)\[\]\{\}\.\!\?\,\;\:\+\=\-_\"\'\`\~\#\@\&\*\%€\$\§\\\/]+"

    def mapper(self, _, line):
        """
        Parses one review and emits term-category counts.

        Input:
            <offset, review_json>

        Output:
            <(token, category), 1>
            <("__TOTAL__", category), 1>
        """

        # Parse one JSON review record.
        review = json.loads(line)

        # Extract the product category.
        category = review["category"]

        # Convert review text to lowercase
        text = review["reviewText"].lower()

        # Split text into candidate tokens using the delimiter regex.
        tokens = re.split(self.delimiters, text)

        # Remove empty tokens, stopwords and one-letter tokens.
        valid_tokens = [t for t in tokens if t and t not in self.stopwords and len(t) > 1]

        # Use set() so a word is counted only once per review.
        for token in set(valid_tokens):
            yield (token, category), 1

        # Count total number of reviews in each category.
        yield ("__TOTAL__", category), 1

    def combiner(self, key, values):
        """
        Sums counts before sending them to the reducers.

        Input:
            <key, [counts]>

        Output:
            <key, local_sum>
        """

        yield key, sum(values)

    def reducer(self, key, values):
        """
        Aggregates all counts for each key across the entire dataset.

        Input:
            <key, [partial_counts]>

        Output:
            <key, total_count>
        """

        yield key, sum(values)

if __name__ == '__main__':
    """
    Entry point of the script.

    This allows the job to be executed from the command line.
    """

    AmazonReviewCounter.run()