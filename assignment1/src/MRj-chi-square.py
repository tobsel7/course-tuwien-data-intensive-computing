from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import json
import heapq


class ChiSquareTopTerms(MRJob):
    """
    MapReduce job for calculating Chi-Square scores and selecting the top terms
    for each product category.

    This job uses the output of amazon-review-counter.py as input.

    It receives document frequencies for each <term, category> pair and total
    document counts for each category. Based on these counts, it builds a
    2x2 table for every term-category pair and calculates the Chi-Square value.

    Only the top 75 terms with the highest Chi-Square scores are kept for each
    category.

    Input:
        <(term, category), document_frequency>
        <("__TOTAL__", category), category_total>

    Mapper output:
        <("0_TOTAL", category), category_total>
        <("1_TERM", term), (category, document_frequency)>

    Partition / Sort / Shuffle:
        Hadoop sorts keys and sends equal keys to the same reducer.
        The prefix "0_TOTAL" ensures that category totals are processed before
        normal term counts, because "0_TOTAL" is sorted before "1_TERM".

    Reducer processing:
        For total keys:
            stores the number of reviews per category.

        For term keys:
            collects all category counts for the current term and calculates Chi-Square scores against all categories.

    Reducer final output:
        category term1:score1 term2:score2 ... term75:score75
        final line: all selected terms in alphabetical order
    """

    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        """
        Defines the MapReduce steps of the job.

        This job uses one MapReduce step:
            mapper:
                parses the output of the previous MapReduce job.

            reducer:
                stores category totals and calculates Chi-Square scores.

        The number of reducers is set to 1 because the final output needs one
        global top-75 list per category and one global merged vocabulary.
        """

        return [
            MRStep(
                mapper=self.mapper_parse_output,
                reducer_init=self.reducer_init,
                reducer=self.reducer_chi_square,
                reducer_final=self.reducer_final,
                jobconf={
                    "mapreduce.job.reduces": "1"
                }
            )
        ]

    def mapper_parse_output(self, _, line):
        """
        Parses one line from the output of AmazonReviewCounter.

        Input:
            <key_json, count>

        Output:
            For category totals:
                <("0_TOTAL", category), category_total>

            For normal terms:
                <("1_TERM", term), (category, document_frequency)>
        """

        key_json, count_string = line.split("\t", 1)
        term, category = json.loads(key_json)
        count = int(count_string)

        if term == "__TOTAL__":
            yield ("0_TOTAL", category), count
        else:
            yield ("1_TERM", term), (category, count)

    def reducer_init(self):
        """
        Initializes data structures used by the reducer.

        cat_totals:
            Stores the total number of reviews per category.

        total_n:
            Stores the total number of reviews over all categories.

        top_terms:
            Stores one heap per category. Each heap keeps at most 75 terms with the highest Chi-Square scores.

        vocabulary:
            Stores all selected top terms from all categories. It is used to create the final merged dictionary line.
        """

        self.cat_totals = {}
        self.total_n = 0
        self.top_terms = {}
        self.vocabulary = set()

    def reducer_chi_square(self, key, values):
        """
        Processes grouped mapper output.

        Input:
            <("0_TOTAL", category), [category_total, ...]>
            <("1_TERM", term), [(category, document_frequency), ...]>

        For "0_TOTAL" keys:
            sums and stores the number of reviews in the category.

        For "1_TERM" keys:
            collects the document frequency of the term in every category and passes these counts to calculate_scores().
        """

        key_type, key_value = key

        if key_type == "0_TOTAL":
            total = sum(values)
            self.cat_totals[key_value] = total
            self.total_n += total
            self.top_terms[key_value] = []
            return

        term = key_value
        counts = {}

        for category, count in values:
            counts[category] = counts.get(category, 0) + count

        self.calculate_scores(term, counts)

    def calculate_scores(self, term, counts):
        """
        Calculates Chi-Square scores for one term against all categories.

        For every category, the method builds a 2x2 table:

                            term present      term absent
            in category          a                c
            outside category     b                d

        a:
            Number of reviews in this category containing the term.

        b:
            Number of reviews outside this category containing the term.

        c:
            Number of reviews in this category not containing the term.

        d:
            Number of reviews outside this category not containing the term.

        Chi-Square:
            chi^2 = N * (a*d - b*c)^2 / ((a+b) * (c+d) * (a+c) * (b+d))
        """

        total_term_count = sum(counts.values())
        total_n = self.total_n
        top_terms = self.top_terms

        for category, category_total in self.cat_totals.items():
            a = counts.get(category, 0)
            b = total_term_count - a
            c = category_total - a
            d = total_n - a - b - c

            denominator = (a + b) * (c + d) * (a + c) * (b + d)

            if denominator == 0:
                chi_square = 0.0
            else:
                chi_square = total_n * ((a * d - b * c) ** 2) / denominator

            heap = top_terms[category]
            item = (chi_square, term)

            if len(heap) < 75:
                heapq.heappush(heap, item)
            elif item > heap[0]:
                heapq.heapreplace(heap, item)

    def reducer_final(self):
        """
        Writes the final result.

        For each category, the top 75 terms are sorted by:
            1. descending Chi-Square score
            2. alphabetical order for terms

        Output format:
            category term1:score1 term2:score2 ... term75:score75

        The last output line contains all selected terms from all categories,
        sorted alphabetically.
        """

        vocabulary = self.vocabulary

        for category in sorted(self.top_terms):
            terms = sorted(
                self.top_terms[category],
                key=lambda item: (-item[0], item[1])
            )

            output_parts = [category]

            for chi_square, term in terms:
                vocabulary.add(term)
                output_parts.append(f"{term}:{chi_square:.4f}")

            yield None, " ".join(output_parts)

        yield None, " ".join(sorted(vocabulary))


if __name__ == "__main__":
    """
    Entry point of the script.
    """

    ChiSquareTopTerms.run()