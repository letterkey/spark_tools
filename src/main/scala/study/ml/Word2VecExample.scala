package study.ml

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.Vector

/**
  * Created by didi on 17/8/23.
  */
object Word2VecExample {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Word2Vec example")
        .master("local")
      .getOrCreate()

    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setWindowSize(5) // 考虑一个单词的前五个和后五个词语
      .setMinCount(0) // 设置一个词最低频率，如果设置为3则出现次数<3则丢弃

    val model = word2Vec.fit(documentDF)

    model.save("data/word2vec/model")
    val result = model.transform(documentDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }
    // $example off$

    val x = result.distinct()
    x.printSchema()
      x.collect().foreach(r => println(r(0),r(1)))

    val saveModel = Word2VecModel.load("data/word2vec/model")
    // 查找跟"I"相关的5个单词 的匹配度
    saveModel.findSynonyms("I",5).foreach(println(_))
    spark.stop()
  }

}
