import pyspark
import sys
import nltk

nltk.download('stopwords')
stop_words = set(nltk.corpus.stopwords.words('english'))


def remove_stop_words(line):
    """
    Function to filter out stop words provided by NLTK package
    """
    words_list = []
    text_list = nltk.tokenize.RegexpTokenizer(r'\w+').tokenize(line)
    for word in text_list:
        word = word.encode("ascii", "ignore").lower()
        if word not in stop_words:
            words_list.append(word)
    return words_list


sc = pyspark.SparkContext()
lines = sc.textFile("gs://big-data-eecs6893/input/shakes.txt")
words_filter = lines.flatMap(remove_stop_words)
words_kv = words_filter.map(lambda x: (x,1))

# Add values of RDD by key and sort
counts = words_kv.reduceByKey(lambda v1, v2: v1 + v2).sortBy(lambda x: -x[1])
print(counts.take(10))

# Alternate way
"""
if __name__ == '__main__':
    input_uri = "gs://big-data-eecs6893/input/shakes.txt"

    sc = pyspark.SparkContext()
    txt_file = sc.textFile(input_uri)
    words = txt_file.flatMap(lambda line: line.split()).filter(lambda x: x!= '')
    words = words.map(lambda word : word.lower())

    text_filter = words.filter(lambda x: x not in stop_words)
    words_kv = text_filter.map(lambda x: (x,1))

    # Add values of RDD by key and sort
    counts = words_kv.reduceByKey(lambda v1, v2: v1 + v2).sortBy(lambda x: -x[1])
    print(counts.take(10))
"""
