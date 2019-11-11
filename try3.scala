package FeatureExtractionAndTransformation

import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.{SparkContext, SparkConf}

val sc = new SparkContext(conf)
// Document RDD from dataset (in text file)
val documents = sc.textFile("4star.txt").map(_.split(" ").toSeq)
// Term Frequeny Vector (in RDD<Vector>)
val tf = new HashingTF().transform(documents)
// Persist the RDD with deafult storage level (MEMORY_ONLY)
tf.cache()

// Dump each term frequency
tf.foreach(println)

// Inverse Document Frequency (IDF) Matrix
val idf = new IDF().fit(tf)
// TF-IDF
val tfidf = idf.transform(tf)

println("tfidf: ")
// tfidf.foreach(x => println(x))
tfidf.saveAsTextFile("hello.txt")

// Exit the program
System.exit(0)
