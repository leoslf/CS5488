import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, Tokenizer, RegexTokenizer, StopWordsRemover, IDF, IDFModel}
import org.apache.spark.ml.linalg.{Vector, DenseVector, SparseVector, Matrix, Matrices, SparseMatrix}
// import org.apache.spark.ml.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, Column}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import spark.implicits._

def json_to_df(path: String, columns: Array[String] = Array("*")): DataFrame = {
  val df = spark.read.json(path)
  return df.select(columns.head, columns.tail: _*).withColumn("document_id", monotonically_increasing_id())
}

val toDense = udf((xs: Vector) => xs.toDense)
def vectorsum(a: Array[Double], b: Array[Double]): Array[Double] = {
  return (a, b).zipped.map(_ + _)
}

val vector2array = udf((v: SparseVector) => v.toArray)

val agg_vectorsum = (df: DataFrame, column: Column) => df.select(column).rdd.map { case Row(v: Vector) => v.toDense.toArray }.reduce(vectorsum)

val activeEntries = udf((v: SparseVector) => v.numActives)

/*
def sparsevectorrdd_to_sparsematrix(vectors: Array[SparseVector], columns: Array[String]): SparseMatrix = {
  val (colptrs: Array[Array[Int]], rowindices: Array[Array[Int]], values: Array[Array[Double]]) = vectors.zipWithIndex.map { case (v: SparseVector, i: Int) => (Array(v.indices.length), v.indices, v.values) }.unzip3
  val colPtrs: Array[Int] = (Array(0) ++ (colptrs.flatten[Int])).map{ var acc = 0; value => {acc += value; acc} }
  return new SparseMatrix(vectors.length, columns.size, colPtrs, rowindices.flatten, values.flatten, true)
}
*/

def vectorColToDF(df: DataFrame, column_name: String, header: Array[String]): DataFrame = {
  val df_ = df.withColumn("tmp", vector2array(col(column_name)))
  val sqlexpr = header.zipWithIndex.map { case (alias, index) => $"tmp".getItem(index).as(alias) }
  return df_.select(sqlexpr : _*)
}

def transform(df: DataFrame, inputColumn: String = "reviewText", idColumn: String = "id"): (DataFrame, DataFrame, Array[String], DataFrame) = { 
  val tokenizer = new RegexTokenizer().setInputCol(inputColumn).setOutputCol("token").setPattern("\\W")
  val stopwordsremover = new StopWordsRemover().setInputCol("token").setOutputCol("words")
  val countvectorizer = new CountVectorizer().setInputCol("words").setOutputCol("tf")
  val idf = new IDF().setInputCol("tf").setOutputCol("tfidf")
  val lda = new LDA().setK(10).setMaxIter(10).setFeaturesCol("tf").setTopicDistributionCol("lda")

  val stages: Array[PipelineStage] = Array(tokenizer, stopwordsremover, countvectorizer, idf, lda)
  val pipeline = new Pipeline().setStages(stages)
  val model = pipeline.fit(df)

  val terms_model: CountVectorizerModel = model.stages(stages.indexOf(countvectorizer)).asInstanceOf[CountVectorizerModel]
  val idf_model: IDFModel = model.stages(stages.indexOf(idf)).asInstanceOf[IDFModel]
  val lda_model: LDAModel = model.stages(stages.indexOf(lda)).asInstanceOf[LDAModel]

  // val df_by_documents = model.transform(df).select($"document_id", $"tf", $"tfidf", $"lda")
  val df_by_documents = model.transform(df).select($"document_id", $"tf", $"tfidf") // , activeEntries($"tf") as "n")

  val df_count = df.count()

  val terms = terms_model.vocabulary

  // val tf: Matrix = sparsevectorrdd_to_sparsematrix(df_by_documents.select("tf").rdd.map { case Row(v: SparseVector) => v }.collect, terms)
  // val tfidf: Matrix = sparsevectorrdd_to_sparsematrix(df_by_documents.select("tfidf").rdd.map { case Row(v: SparseVector) => v }.collect, terms)

  val log_D_plus1 = math.log(df_count + 1)

  val df_by_terms: DataFrame = (terms, idf_model.idf.toArray).zipped.toSeq.toDF("term", "idf").withColumn("df", round(exp(-$"idf" + lit(log_D_plus1)) - lit(1)))

  val toWords = udf((x : Seq[Int]) => { x.map(i => terms(i)) })

  val df_lda = lda_model.describeTopics(10).withColumn("topicWords", toWords($"termIndices")).select("topicWords", "termWeights")


  return (df_by_terms, df_by_documents, terms, df_lda) 
}

val dataframe = json_to_df("musical.json", columns = Array("reviewText", "overall"))
val rating_dfs: Map[Any, (DataFrame, DataFrame, Array[String], DataFrame)] = List(1, 2, 3, 4, 5, "overall").map(rating => (rating, transform(if (rating.isInstanceOf[String]) dataframe else dataframe.where($"overall" === rating)))).toMap

for ((rating, (df_by_terms: DataFrame, df_by_documents: DataFrame, terms: Array[String], df_lda: DataFrame)) <- rating_dfs) {
  val basename: String = "project/" + (if (rating.isInstanceOf[String]) "overall" else s"rating_$rating")
  df_by_terms.write.mode("overwrite").option("header", "true").format("csv").save(s"$basename.terms.csv")
  df_by_documents.write.mode("overwrite").format("json").save(s"$basename.documents.json")

  vectorColToDF(df_by_documents, "tf", terms).write.mode("overwrite").option("header", "true").format("csv").save(s"$basename.tf.csv")
  vectorColToDF(df_by_documents, "tfidf", terms).write.mode("overwrite").option("header", "true").format("csv").save(s"$basename.tfidf.csv")

  df_lda.write.mode("overwrite").format("json").save(s"$basename.lda.json")
}

// Exit the program
System.exit(0)
