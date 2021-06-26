import re
from pyspark.sql import SparkSession


def normalize_words(text: str):
    return re.compile(r"\W+", re.UNICODE).split(text)


session = SparkSession.builder.appName("WordCounter").getOrCreate()
session.sparkContext.setLogLevel("ERROR")

book = session.sparkContext.textFile("../src/resources/DaddyLongLeg_book_korean.txt")
words = book.flatMap(normalize_words)
word_counts = words.countByValue()

for word, count in sorted(word_counts.items(), key=lambda x: x[1]):
    cleanWord = word.encode("utf-8", "ignore")
    if count > 10:
        print(cleanWord.decode("utf-8"), count)

session.stop()
