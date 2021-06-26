from pyspark.sql import SparkSession

session = SparkSession.builder.appName("wordCounter").getOrCreate()
session.sparkContext.setLogLevel("ERROR")

book = session.sparkContext.textFile("../src/resources/DaddyLongLeg_book_korean.txt")
words = book.flatMap(lambda line: line.split())
word_counts = words.countByValue()

# most frequently used word
mfuw = max(word_counts, key=lambda word: word_counts[word])
print(mfuw)

for word, count in sorted(word_counts.items(), key=lambda word: word[1]):
    cleanWord = word.encode("utf-8", "ignore")
    if count > 10:
        print(cleanWord.decode("utf-8"), count)

session.stop()
