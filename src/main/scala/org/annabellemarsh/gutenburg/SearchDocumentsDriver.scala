package org.annabellemarsh.gutenberg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SearchDocumentsDriver extends Serializable {
  val spark = SparkSession
    .builder()
    .appName("Search Documents")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]) = {
    // parse args. Args should be of the format `{inputPath} {word} --limit {limit}`
    val inputPath = args(0)
    val word = args(1)
    val word_count_col_name = s"${word}_count"
    val limit = args(3).toInt

    val textDf = spark.read.parquet(inputPath)

    /* 
      create new column with the number of times {word} appears in that text
      select all columns besides "text"
      sort descending by word_count
    */
    val finalDf = textDf
      .withColumn(word_count_col_name, size(filter(col("text"), (text) => text === word)))
      .select("title", "author", "id", word_count_col_name)
      .sort(desc(word_count_col_name))

    finalDf.show(limit, truncate = false) // display the dataframe in the logs
  }
}