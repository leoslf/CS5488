import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, IDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row}

def json_to_rdd(path: String, columns: Array[String] = Array("*")): DataFrame = {
  val df = spark.read.json(path)
  // Creates a temporary view using the DataFrame
  df.createOrReplaceTempView("tmp")

  val columns_string: String = columns.mkString(", ")

  return spark.sql(f"SELECT $columns_string%s FROM tmp")
}

val df = json_to_rdd("musical.json", columns = Array("reviewText", "overall"))
// df.collect.foreach(println)


val tokenizer = new Tokenizer().setInputCol("reviewText").setOutputCol("token")
val hashingTF = new HashingTF().setInputCol("token").setOutputCol("tf")
val idf = new IDF().setInputCol("tf").setOutputCol("idf")

val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf))
val model = pipeline.fit(df)


var df_transform = model.transform(df)
println("Schema")
println(df_transform.schema)

// TF-IDF
println("tf-idf")
model.transform(df).collect.foreach(println)

// Exit the program
System.exit(0)
