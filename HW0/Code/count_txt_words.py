import pyspark
import sys

# Cloud storage URI where the text file is stored
input_uri = "gs://big-data-eecs6893/input/shakes.txt"

sc = pyspark.SparkContext.getOrCreate()
txt_file = sc.textFile(input_uri)

# Split lines to words, and create key value pairs
words = txt_file.flatMap(lambda line:line.encode("ascii", "ignore").split())
words_kv = words.map(lambda word: (word, 1))

# Add values of RDD by key and sort
counts = words_kv.reduceByKey(lambda v1, v2: v1 + v2).sortBy(lambda x: -x[1])
print(counts.take(10))