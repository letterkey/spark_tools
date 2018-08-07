package study.ml

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * Created by didi on 17/8/23.
  */
object TfIdfExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("TfIdfExample")
        .master("local")
      .getOrCreate()

    // $example on$
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)


    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    featurizedData.select("words").collect().foreach(print)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)


    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.printSchema()
    rescaledData.select("words","sentence","label", "features").show(false)
    // $example off$

    featurizedData.collect().foreach(println(_))
    spark.stop()
  }
}
