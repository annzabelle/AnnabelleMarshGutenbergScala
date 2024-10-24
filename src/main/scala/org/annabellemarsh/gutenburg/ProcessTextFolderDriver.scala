package org.annabellemarsh.gutenberg

import org.apache.spark.sql.SparkSession

object ProcessTextFolderDriver extends Serializable {
  val spark = SparkSession
    .builder()
    .appName("Process Text Folder")
    .master("local")
    .getOrCreate()

  def getBookText(value: String) = { // convert the string of the entire book to an array of words
    value.split(Array('\r','\n',' ')).filterNot(_.equals("")).toSeq
  }

  def getBookAuthor(text: Seq[String]) = { // parse the author from the gutenberg boilerplate
    val startAuthorIndex = text.indexWhere(_ == "Author:")
    val endAuthorIndex = text.indexWhere(_ == "Release", startAuthorIndex)
    text.slice(startAuthorIndex + 1, endAuthorIndex).mkString(" ")
  }

  def getBookTitle(text: Seq[String]) = { // parse the title from the gutenberg boilerplate
    val startTitleIndex = text.indexWhere(_ == "Title:")
    val endTitleIndex = text.indexWhere(_ == "Author:", startTitleIndex)
    text.slice(startTitleIndex + 1, endTitleIndex).mkString(" ")
  }

  def getBookId(text: Seq[String]) = { // parse the id from the gutenberg boilerplate
    val idIndex = text.indexWhere(_ == "[eBook") + 1
    text(idIndex).filterNot(Set('#', ']')).toInt
  }

  def main(args: Array[String]) = {
    // parse args - args should be of the format {inputPath} {outputPath}
    val inputPath = args(0)
    val outputPath = args(1)

    spark.udf.register("getBookText", ProcessTextFolderDriver.getBookText(_:String):Seq[String])
    spark.udf.register("getBookAuthor", ProcessTextFolderDriver.getBookAuthor(_:Seq[String]):String)
    spark.udf.register("getBookTitle", ProcessTextFolderDriver.getBookTitle(_:Seq[String]):String)
    spark.udf.register("getBookId", ProcessTextFolderDriver.getBookId(_:Seq[String]):Int)

    // read the text files from the input path as rows in a df
    val wholeTextDf = spark.read.option("wholetext", true).text(inputPath)

    // convert the text into an array and parse the array for gutenberg boilerplate
    val finalDf = wholeTextDf.selectExpr("getBookText(value) as text") 
      .selectExpr("getBookTitle(text) as title", 
      "getBookAuthor(text) as author",
      "getBookId(text) as id",
      "text as text")

    finalDf.write.parquet(outputPath)
  }
}
