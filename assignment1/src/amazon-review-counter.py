from mrjob.job import MRJob
import json
import re

class AmazonReviewCounter(MRJob):

    def configure_args(self):
        super(AmazonReviewCounter, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords.txt")

    def mapper_init(self):
        with open(self.options.stopwords, "r") as file:
            self.stopwords = set(file.read().split())
        self.delimiters = r"[\s\d\(\)\[\]\{\}\.\!\?\,\;\:\+\=\-_\"\'\`\~\#\@\&\*\%€\$\§\\\/]+"

    def mapper(self, _, line):
        review = json.loads(line)
        category = review["category"]
        text = review["reviewText"].lower()

        tokens = re.split(self.delimiters, text)
        valid_tokens = [t for t in tokens if t and t not in self.stopwords and len(t) > 1]

        for token in set(valid_tokens):
            yield (token, category), 1

        yield ("__TOTAL__", category), 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    AmazonReviewCounter.run()