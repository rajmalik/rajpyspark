import re
from functools import partial
from shared.context import JobContext

__author__ = 'ekampf'

class RajBQContext(JobContext):
    def _init_accumulators(self, sc):
        self.initalize_counter(sc, 'words')


strip_regexp = re.compile(r"[^\w]*")

def to_pairs(context, word):
    context.inc_counter('words')
    return word, 1


def analyze(sc):
    print("Running rajbigquery")
    bucket = "schema-inference-outprotodf"
    sc.conf.set('temporaryGcsBucket', bucket)

    # Load data from BigQuery.
    words = sc.read.format('bigquery') \
        .option('table', 'bigquery-public-data:samples.shakespeare') \
        .load()
    words.createOrReplaceTempView('words')

    # Perform word count.
    word_count = sc.sql(
        'SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word')
    word_count.show()
    word_count.printSchema()

    # Saving the data to BigQuery
    word_count.write.format('bigquery') \
        .option('table', 'schema_infer.test_output') \
        .option('mode', SaveMode.Override) \
        .save()



