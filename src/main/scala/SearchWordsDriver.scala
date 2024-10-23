package org.annabellemarsh.gutenburg

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apahce.spark.sql.functions._

object SearchWordsDriver extends Serializable {
  def getWordFrequency (text: Seq[String]): Map[String, Int] = {
    text.groupMapReduce(word => word)(_ => 1)(_ + _) // converts array of words to map of words to their frequencies
  }

  def main(args: Array[String]) = {
    spark.udf.register("getWordFrequency", SearchWordsDriver.getWordFrequency(_:Seq[String]):Map[String, Int])

    val inputPath = args(0)
    val id = args(1)
    val limit = args(3).toInt
    
    val word_count_col_name = s"${word}_count"

    val spark = SparkSession
      .builder()
      .appName("Process Gutenberg Text")
      .getOrCreate()
    import spark.implicits._

    val textDf = spark.read.parquet(inputPath)
    val finalDf = textDf.filter(textDf("id") === id)
        .selectExpr("title as title", "author as author", "id as id", "getWordFrequency(text) as wordFrequency")
        .select($"title", $"author", $"id", explode($"wordFrequency"))
        .toDF("title", "author", "id", "word", "frequency")
        .sort(desc("frequency"))
    finalDf.show(limit, truncate = false)
  }
}