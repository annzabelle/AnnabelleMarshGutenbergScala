package org.annabellemarsh.gutenburg

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
object ProcessTextFolderDriver extends Serializable {
  def getBookText (value: String) = {
    value.split(Array('\r','\n',' ')).filterNot(_.equals("")).toSeq
  }

  def getBookAuthor (text: Seq[String]) = {
    val startAuthorIndex = text.indexWhere(_ == "Author:")
    val endAuthorIndex = text.indexWhere(_ == "Release", startAuthorIndex)
    text.slice(startAuthorIndex + 1, endAuthorIndex).mkString(" ")
  }

  def getBookTitle (text: Seq[String]) = {
    val startTitleIndex = text.indexWhere(_ == "Title:")
    val endTitleIndex = text.indexWhere(_ == "Author:", startTitleIndex)
    text.slice(startTitleIndex + 1, endTitleIndex).mkString(" ")
  }

  def getBookId (text: Seq[String]) = {
    val idIndex = text.indexWhere(_ == "[eBook") + 1
    text(idIndex).filterNot(Set('#', ']')).toInt
  }

  def main(args: Array[String]) = {
    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession
      .builder()
      .appName("Process Gutenberg Text")
      .getOrCreate()
    import spark.implicits._

    spark.udf.register("getBookText", ProcessTextFolder.getBookText(_:String):Seq[String])
    spark.udf.register("getBookAuthor", ProcessTextFolder.getBookAuthor(_:Seq[String]):String)
    spark.udf.register("getBookTitle", ProcessTextFolder.getBookTitle(_:Seq[String]):String)
    spark.udf.register("getBookId", ProcessTextFolder.getBookId(_:Seq[String]):Int)

    val wholeTextDf = spark.read.option("wholetext", true).text(inputPath)
    val finalDf = wholeTextDf.selectExpr("getBookText(value) as text").
      selectExpr("getBookTitle(text) as title", 
      "getBookAuthor(text) as author",
      "getBookId(text) as id",
      "text as text")

    finalDf.write.parquet(outputPath)
  }
}
