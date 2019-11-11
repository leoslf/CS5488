// package com.cs5488.FeatureExtractionAndTransformation

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.types.StructType
// import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.feature.{IDF}
import org.apache.spark.mllib.linalg.Vector

/*
var conf = new SparkConf().set("spark.hadoop.validateOutputSpecs", "false")
val sc = new SparkContext(conf)

// Document RDD from dataset (in text file)
// val documents: RDD[Seq[String]] = sc.textFile("4star.txt").map(_.split(" ").toSeq)
// val documents: 

val tokenizer = new Tokenizer()

// Term Frequeny Vector (in RDD<Vector>)
val hashingTF = new HashingTF()
val tf: RDD[Vector] = hashingTF.transform(documents)
// Persist the RDD with deafult storage level (MEMORY_ONLY)
tf.cache()

documents.foreach(println)

// Dump each term frequency
tf.collect.saveAsTextFile("project_results/tf.txt")

// Inverse Document Frequency (IDF) Matrix
val idf = new IDF().fit(tf)
// TF-IDF
val tfidf: RDD[Vector] = idf.transform(tf)

println("tfidf: ")
// tfidf.foreach(x => println(x))
tfidf.saveAsTextFile("project_results/hello.txt")
*/

case class Document(text: String)

// TODO: change the object name
object ProjectApp {
  // Set log level of "org.*" to ERROR to be less verbose
  // Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf().set("spark.hadoop.validateOutputSpecs", "false").setMaster("local")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {
    val inputFile = "4star.txt" // args(0)

    // RDD to SchemaRDD implicit conversion
    import sqlContext.createSchemaRDD

    // val df = sc.parallelize(sc.textFile(inputFile).map(Document).collect)
    val df = sc.textFile(inputFile).map(line => Document(line))
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("terms")
    val hashingTF = new HashingTF().setInputCol("terms").setOutputCol("features")
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF))
    val model = pipeline.fit(df)

    var df_transform = model.transform(df)
    println("test1")
    println(df_transform.schemaString)

    println("test2")
    model.transform(df).collect.foreach(println)
  }
}


val sparkConf = new SparkConf().set("spark.hadoop.validateOutputSpecs", "false").setMaster("local")
val sc = new SparkContext(sparkConf)
val sqlContext = new SQLContext(sc)

val inputFile = "4star.txt" // args(0)

// RDD to SchemaRDD implicit conversion
import sqlContext.createSchemaRDD

// val df = sc.parallelize(sc.textFile(inputFile).map(Document).collect)
val df = sc.textFile(inputFile).map(line => Document(line))
val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("terms")
val hashingTF = new HashingTF().setInputCol("terms").setOutputCol("features")
val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF))
val model = pipeline.fit(df)

var df_transform = model.transform(df)
println("test1")
println(df_transform.schemaString)

println("test2")
model.transform(df).collect.foreach(println)




// Exit the program
System.exit(0)
