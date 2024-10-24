package org.annabellemarsh.gutenberg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SearchWordsDriver extends Serializable {
  val spark = SparkSession
    .builder()
    .appName("Search Words")
    .master("local")
    .getOrCreate()

  // converts array of words to map of words to their frequencies
  def getWordFrequency (text: Seq[String]): Map[String, Int] = {
    text.groupMapReduce(word => word)(_ => 1)(_ + _) 
  }

  def main(args: Array[String]) = {
    spark.udf.register("getWordFrequency", SearchWordsDriver.getWordFrequency(_:Seq[String]):Map[String, Int])

    // parse args. Args should be of the format `{inputPath} {book_id} --limit {limit}`
    val inputPath = args(0)
    val book_id = args(1)
    val limit = args(3).toInt

    val textDf = spark.read.parquet(inputPath)
    val finalDf = textDf.filter(textDf("id") === book_id) // filter to just the book we're referencing
        .selectExpr("getWordFrequency(text) as wordFrequency") // get the word frequency map
        .select(explode(col("wordFrequency"))) //explode the new wordFrequency map
        .toDF("word", "frequency")
        .sort(desc("frequency")) // sort by the exploded word frequencies
    finalDf.show(limit, truncate = false) // display the dataframe in the logs
  }
}