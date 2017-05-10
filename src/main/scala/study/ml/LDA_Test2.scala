package study.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
/**
  * Created by yinmuyang on 17/4/18.
  */
object LDA_Test2 {

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("LDA_Test").setMaster("local")
        val sc = new SparkContext(conf)
        // Load documents from text files, 1 document per file
        val path = "/Users/yinmuyang/git/spark/docs/*.md"
        val modelPath = "target/model/lda/"
        val tokenized = preprocess(sc,path)
        train(sc,tokenized,modelPath)


    }

    /**
      * 训练模型
      * @param sc
      * @param tokenized
      * @param modelPath
      */
    def train(sc : SparkContext, tokenized : RDD[Seq[String]],modelPath : String) : Unit ={
        // Choose the vocabulary. termCounts: Sorted list of (term, termCount) pairs
        val termCounts: Array[(String, Long)] = tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
        //   vocabArray: Chosen vocab (removing common terms)
        val numStopwords = 20
        // 需要将词索引持久化
        val vocabArray: Array[String] = termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
        //   vocab: Map term -> term index
        val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
        sc.parallelize(vocabArray).saveAsTextFile("target/model/tokenized")
        // Convert documents into term count vectors
        val documents: RDD[(Long, Vector)] =
            tokenized.zipWithIndex.map { case (tokens, id) =>
                val counts = new mutable.HashMap[Int, Double]() // K:word对应的id,V: word出现的次数
                tokens.foreach { term =>
                    if (vocab.contains(term)) {
                        val idx = vocab(term)
                        counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
                    }
                }
                // println(vocab.size,counts.toSeq)
                // 稀疏向量:每个文档id对应的words 稀疏向量
                (id, Vectors.sparse(vocab.size, counts.toSeq))
            }

        // Set LDA parameters
        // 设置topic的个数
        val numTopics = 10
        val lda = new LDA().setK(numTopics).setMaxIterations(10)

        val ldaModel = lda.run(documents)
        //        val avgLogLikelihood = ldaModel.logLikelihood / documents.count()

        // Print topics, showing top-weighted 10 terms for each topic.
        // 指定每个topic中返回的此数量(已经按照权重降序排列)
        val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
        topicIndices.foreach {
            case (terms, termWeights) =>
                println("TOPIC:")
                terms.zip(termWeights).foreach {
                    case (term, weight) =>
                        println(s"${vocabArray(term.toInt)}: $weight")
                }
                println()
        }

        ldaModel.save(sc,modelPath)
    }

    /**
      * 预测
      * @param sc
      * @param tokenized
      * @param modelPath
      */
    def prediction(sc : SparkContext,tokenized : RDD[Seq[String]],modelPath:String): Unit ={
        val sameModel = DistributedLDAModel.load(sc,modelPath)
        val vocab = sc.textFile("target/model/tokenized/").zipWithIndex()
        // 预测文档在topic上的分布:预测新文档主题分布
        val docment: RDD[(Long, Vector)] = tokenized.zipWithIndex().map{
            case (tokens,id) =>{
                val counts = new mutable.HashMap[Int, Double]() // K:word对应的id,V: word出现的次数
                tokens.foreach { term =>
                    if (vocab.contains(term)) {
                        val idx = vocab(term)
                        counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
                    }
                }
                // println(vocab.size,counts.toSeq)
                // 稀疏向量:每个文档id对应的words 稀疏向量
                (id, Vectors.sparse(vocab.size, counts.toSeq))
            }
        }
        val r = sameModel.toLocal.topicDistributions(docment)
        //        sameModel.toLocal.topics.multiply()
        for(doc <- sameModel.topicAssignments){
            println(doc._1+"    "+doc._2+"    "+doc._3)
        }
    }
    /**
      * 文本预处理
      * @param sc
      * @param path
      * @return
      */
    def preprocess(sc : SparkContext,path : String) : RDD[Seq[String]] ={
        val data = sc.wholeTextFiles("/Users/yinmuyang/git/spark/docs/*.md")
        data.map(_._1).collect().foreach(println)
        val corpus: RDD[String] = data.map(_._2)

        // Split each document into a sequence of terms (words)
        // 根据空格将文本内容分词后并过滤出char的组合长度大于3并且是字母的 words
        val tokenized: RDD[Seq[String]] = corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))
        tokenized
    }
}