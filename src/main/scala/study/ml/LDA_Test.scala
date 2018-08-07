package study.ml

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yinmuyang on 17/4/18.
  */
object LDA_Test {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("LDA_Test").setMaster("local")
        val sc = new SparkContext(conf)
        // 输入的文件每行用词频向量表示一篇文档
        val data = sc.textFile("data/mllib/sample_lda_data.txt")
        // Vectors.dense:稠密向量
        val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
        // zipWithIndex,将rdd中的元素和这个元素在rdd中的ID(索引号)组成键值对;
        // _.swap 将键值对如(1,2) 转换为(2,1) 键值对翻转
        val corpus = parsedData.zipWithIndex.map(_.swap).cache()

        val ldaModel = new LDA().setK(3).run(corpus)

        // 打印主题
        println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
        val topics = ldaModel.topicsMatrix

        for (topic <- Range(0, 3)) {
            print("Topic " + topic + ":")
            for (word <- Range(0, ldaModel.vocabSize)) {
                print(" " + topics(word, topic)); }
            println()
        }
        ldaModel.describeTopics(3)

        //ldaModel.save(sc,"target/model/lda/")
        //val sameModel = DistributedLDAModel.load("target/model/lda/")
    }
}
