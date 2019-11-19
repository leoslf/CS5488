import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, Tokenizer, RegexTokenizer, StopWordsRemover, IDF}
import org.apache.spark.ml.linalg.{Vector, DenseVector}
import org.apache.spark.sql.{DataFrame, Row, Column}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._

def json_to_rdd(path: String, columns: Array[String] = Array("*")): DataFrame = {
  val df = spark.read.json(path)
  return df.select(columns.head, columns.tail: _*)
}

// val schema = StructType(Seq(
//     StructField("indices", ArrayType(LongType, true), true), 
//     StructField("size", LongType, true),
//     StructField("type", ShortType, true), 
//     StructField("values", ArrayType(DoubleType, true), true)
// ))
// 
// val tf_df_schema = StructType(Seq(StructField("tf_df", schema, true)))
// val tfidf_df_schema = StructType(Seq(StructField("tfidf_df", schema, true)))

// val getitem = ((column: Column, member: String) => from_json(to_json(struct(column)), schema).getItem(member))

// case class Intermediate(_type: ByteType, size: Int, indices: Array[Int], values: Array[Double]) // extends UserDefinedType

// val get_indices = udf((xs: Intermediate) => xs.indices)
// val get_values = udf((xs: Intermediate) => xs.values)

val toDense = udf((xs: Vector) => xs.toDense)
def vectorsum(a: Array[Double], b: Array[Double]): Array[Double] = {
  return (a, b).zipped.map(_ + _)
}

val agg_vectorsum = (df: DataFrame, column: Column) => df.select(column).rdd.map { case Row(v: Vector) => v.toDense.toArray }.reduce(vectorsum)

// User Defined Aggregate Function
def transform(df: DataFrame, inputColumn: String = "reviewText"): (DataFrame, DataFrame) = {
  val tokenizer = new RegexTokenizer().setInputCol(inputColumn).setOutputCol("token").setPattern("\\W")
  val stopwordsremover = new StopWordsRemover().setInputCol("token").setOutputCol("words")
  // val countvectorizer = new CountVectorizer().setInputCol("words").setOutputCol("tf_df")
  // val idf = new IDF().setInputCol("tf_df").setOutputCol("tfidf_df")
  val countvectorizer = new CountVectorizer().setInputCol("words").setOutputCol("tf")
  val idf = new IDF().setInputCol("tf").setOutputCol("tfidf")

  val stages: Array[PipelineStage] = Array(tokenizer, stopwordsremover, countvectorizer, idf)

  val pipeline = new Pipeline().setStages(stages)
  val model = pipeline.fit(df)

  val terms_model: CountVectorizerModel = model.stages(stages.indexOf(countvectorizer)).asInstanceOf[CountVectorizerModel]

  // val transformed = model.transform(df)

  // val outputdf = transformed.select(from_json(to_json(struct($"tf_df")), tf_df_schema).getItem("tf_df").getItem("indices") as "indices", from_json(to_json(struct($"tf_df")), tf_df_schema).getItem("tf_df").getItem("values") as "tf", from_json(to_json(struct($"tfidf_df")), tfidf_df_schema).getItem("tfidf_df").getItem("values") as "tfidf")
  // outputdf.show(truncate=true)

  // return (Seq((terms_model.vocabulary)).toDF("terms"), outputdf)
  val outputdf = model.transform(df).select("tf", "tfidf")

  val corpusdf = (terms_model.vocabulary, agg_vectorsum(outputdf, $"tf"), agg_vectorsum(outputdf, $"tfidf")).zipped.toSeq.toDF("term", "tf", "tfidf") // .select($"term", outputdf.agg(sum($"tf")) as "tf", outputdf.agg(sum($"tfidf")) as "tfidf")
  // outputdf.agg(sum($"tfidf" / $"tf")), outputdf.

  return (corpusdf, outputdf)
}


val dataframe = json_to_rdd("musical.json", columns = Array("reviewText", "overall"))
val rating_dfs: Map[Any, (DataFrame, DataFrame)] = List(1, 2, 3, 4, 5, "overall").map(rating => (rating, transform(if (rating.isInstanceOf[String]) dataframe else dataframe.where($"overall" === rating)))).toMap

for ((rating, (terms: DataFrame, df: DataFrame)) <- rating_dfs) {
  val basename: String = "project/" + (if (rating.isInstanceOf[String]) "overall" else s"rating_$rating")
  terms.repartition(1).write.mode("overwrite").option("header", "true").format("csv").save(s"$basename.terms.csv")
  df.repartition(1).write.mode("overwrite").format("json").save(s"$basename.df.json")
}

// Exit the program
System.exit(0)
