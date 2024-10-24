# Project Gutenberg Assessment
## Annabelle Marsh

## Dependencies

[Scala 2.13.15](https://www.scala-lang.org/download/2.13.15.html) Make sure sbt is installed and working.

[Spark 3.5.3](https://spark.apache.org/downloads.html) Make sure SPARK_HOME is added to .bashrc $PATH. 

This application was developed on MacOS. These instructions should also work on Linux, no gurantees on Windows.

If you have any questions about environment setup, email me at annabellemarsh0@gmail.com.

## Application Architecture

This is a simple implementation of a search/word frequency lookup with 3 separate Spark classes. 
* `ProcessTextFolderDriver` reads a folder of Project Gutenberg text files and parses the title and author, and converts the text into an array of words. This is then stored in an output folder in a parquet format.
* `SearchDocumentsDriver` reads the parquet files of books and input args for a word to search and the number of books returned. This will return a dataframe of books by the number of times the searched word appears.
* `SearchWordsDriver` reads the parquet files of books, takes an argument of a book id, and a limit of how many words to display. It displays the top n words in the book based on how frequently they appear.

## Instructions

Compile the code by running `sbt package`. There should be a jar file at `target/scala-2.13/gutenberg_2.13-0.1-SNAPSHOT.jar`

Run the code using `spark-submit` commands as follows. Feel free to download more txt files from Project Gutenberg to `resources/books` and add more books to the database. Make sure to run `ProcessTextFolderDriver` first to generate the parquet files that the other steps depend on. `spark-submit` generates a lot of logs - the relevant parts for these applications are near the bottom.

1. Convert the text files to parquet: `spark-submit --class org.annabellemarsh.gutenberg.ProcessTextFolderDriver target/scala-2.13/gutenberg_2.13-0.1-SNAPSHOT.jar resources/books resources/books.parquet`

2. View the top words in a book by its id (replace 11 and 10 with the book_id and the limit, respectively): `spark-submit --class org.annabellemarsh.gutenberg.SearchWordsDriver target/scala-2.13/gutenberg_2.13-0.1-SNAPSHOT.jar resources/books.parquet 11 --limit 20` You should see a data frame written near the end of the logs like this:
```
+----+---------+
|word|frequency|
+----+---------+
|the |1683     |
|and |782      |
|to  |778      |
|a   |667      |
|of  |604      |
|she |485      |
|said|416      |
|in  |406      |
|it  |357      |
|was |329      |
+----+---------+
```
only showing top 10 rows

3. View which books have the most copies of a word (replace fish and 5 with the word you're searching for and the limit, respectively)`spark-submit --class org.annabellemarsh.gutenberg.SearchDocumentsDriver target/scala-2.13/gutenberg_2.13-0.1-SNAPSHOT.jar resources/books.parquet fish --limit 5`
```
+-----------------------------------------+---------------------------+----+----------+
|title                                    |author                     |id  |fish_count|
+-----------------------------------------+---------------------------+----+----------+
|Moby Dick; Or, The Whale                 |Herman Melville            |2701|41        |
|The Complete Works of William Shakespeare|William Shakespeare        |100 |32        |
|Pride and Prejudice                      |Jane Austen                |1342|5         |
|Middlemarch                              |George Eliot               |145 |3         |
|Frankenstein; Or, The Modern Prometheus  |Mary Wollstonecraft Shelley|84  |3         |
+-----------------------------------------+---------------------------+----+----------+
```

## Scalability

This application is extremely scalable by running it on a Spark cluster such as AWS EMR. The only code changes needed to run it on EMR would be the `SparkSession` configurations and some read/write configuration for the `books` and `books.parquet` folders. Because I am using spark native functions and UDFs for all processing, this job will be automatically sharded out across any size of cluster. Rather than reading and writing to local Parquet files, I would use alternative data storage/database solutions to scale the file storage if larger scale was needed. Potential options include Cassandra, HDFS, or Hive. 

## Design Decisions/Performance Bottlenecks

I chose to use Apache Spark for this application because it scales well and by running on a sufficiently large cluster, this code would be performant on very large datasets. Scala was chosen over Python because Scala UDFs are much more performant than python ones in Spark, static typing helps with compile-time error checking, and the power of `groupMapReduce` makes the `getWordFrequency` function extremely simple and performant.

I chose to use Spark native functions where possible to benefit from the optimizations, parallel processing, and fault tolerance of Spark, but I used UDFs when Spark native functions did not exist. Spark UDFs are distributed accross nodes, but create potential performance bottlenecks with serialization and deserialization of the array of words, but at the size of books (~100-200k words), this is not significant. 

Rather than getting the word frequency only when running `SearchWordsDriver`, I could move `getWordFrequency` to `ProcessTextFolderDriver` and store the frequency map in the parquet files instead of the whole books. This would reduce the storage space needed for the parquet files, as well as the memory and processing power needed for `SearchDocumentsDriver` and `SearchWordsDriver`. I chose not to do this because I want to maintain more information about the contents of the book in the database rather than just the word frequency map.

The UDFs in `ProcessTextFolderDriver` are slow in that they are each traversing a 100-200k element array, but because of how Spark distributes UDFs, we can have the text array from many rows processing at the same time. 

## Improvements

* Unit testing
* Logging/error handling
* Class to store, parse, and validate args
* Use a metadata source for author, title, and id rather than scraping them from the text (which is slow but constant time when scaled)

## Resources Used

Example books are downloaded from Project Gutenberg.
