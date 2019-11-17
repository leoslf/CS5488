import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, Tokenizer, RegexTokenizer, StopWordsRemover, IDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

def json_to_rdd(path: String, columns: Array[String] = Array("*")): DataFrame = {
  val df = spark.read.json(path)
  return df.select(columns.head, columns.tail: _*)
  // // Creates a temporary view using the DataFrame
  // df.createOrReplaceTempView("tmp")

  // // Concatenate the specified column names with comma
  // val columns_string: String = columns.mkString(", ")

  // // filter out the specified columns from the dataset
  // return spark.sql(f"SELECT $columns_string%s FROM tmp")
}

def preprocessing(df: DataFrame, inputColumn: String, outputColumn: String): DataFrame = {
  // val tokenizer = new Tokenizer().setInputCol(inputColumn).setOutputCol("token")
  val tokenizer = new RegexTokenizer().setInputCol(inputColumn).setOutputCol("token").setPattern("\\W")
  val stopwordsremover = new StopWordsRemover().setInputCol("token").setOutputCol(outputColumn)

  val pipeline = new Pipeline().setStages(Array(tokenizer, stopwordsremover))
  val model = pipeline.fit(df)

  return model.transform(df)
}

def TFIDF(df: DataFrame, inputColumn: String, outputColumn: String): DataFrame = {
  val countvectorizer = new CountVectorizer().setInputCol(inputColumn).setOutputCol("tf")
  val idf = new IDF().setInputCol("tf").setOutputCol(outputColumn)

  val pipeline = new Pipeline().setStages(Array(countvectorizer, idf))
  val model = pipeline.fit(df)

  return model.transform(df)
}

// Register the UDAF with Spark SQL
// spark.udf.register("gm", new GeometricMean)

val dataframe = json_to_rdd("musical.json", columns = Array("reviewText", "overall"))


// User Defined Aggregate Function
def transform(df: DataFrame): DataFrame = {
  val terms = preprocessing(dataframe, "reviewText", "terms").select("terms")
  val tfidf = TFIDF(terms, "terms", "idf")

  // println((overall, tfidf.collect))

  return tfidf
}

val rating_dfs: Map[Int, DataFrame] = (1 to 5).map(rating => (rating, transform(dataframe.where($"overall" === rating)))).toMap
val overall_df = transform(dataframe)

for ((rating, df) <- rating_dfs) {
  // println((rating, df.collect))
  df.repartition(1).write.format("json").option("header", "true").save(s"rating_$rating.txt")
}

// println(("overall", overall_dfs.collect))
overall_df.repartition(1).write.format("json").option("header", "true").save("overall.txt")

// // Schema
// println(tfidf.schema)

// Exit the program
System.exit(0)
