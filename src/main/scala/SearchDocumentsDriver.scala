package org.annabellemarsh.gutenburg

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apahce.spark.sql.functions._
object SearchDocumentsDriver extends Serializable {
  def main(args: Array[String]) = {
    val inputPath = args(0)
    val word = args(1)
    val limit = args(3).toInt
    
    val word_count_col_name = s"${word}_count"

    val spark = SparkSession
      .builder()
      .appName("Process Gutenberg Text")
      .getOrCreate()
    import spark.implicits._

    val textDf = spark.read.parquet(inputPath)
    val finalDf = textDf.withColumn(word_count_col_name, size(filter(textDf("text"), (text) => text === word)))
      .select("title", "author", "id", word_count_col_name)
      .sort(desc(word_count_col_name))
    finalDf.show(limit, truncate = false)
  }
}