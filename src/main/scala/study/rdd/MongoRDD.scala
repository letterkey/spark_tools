package study.rdd


import com.mongodb.client.MongoCursor
import com.mongodb.{MongoClient, MongoClientURI}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.bson.Document

/**
  * Created by yinmuyang on 16/11/3.
  *
**/
class MongoRDD(sc:SparkContext,mongoUrl:String,database:String,collection:String,query:Document,numPartitions:Int)
    extends RDD[Document](sc,Nil) {

    val total:Long = new MongoClient(new MongoClientURI(mongoUrl)).getDatabase(database).getCollection(collection).count(query)

    val batchSize:Int = Math.ceil( total.toDouble / numPartitions.toDouble).toInt

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[Document] = {
        val cursor : MongoCursor[Document] = new MongoClient(new MongoClientURI(mongoUrl))
            .getDatabase(database).getCollection(collection).find(query).iterator()
//        val list : List[Document] = new util.ArrayList[Document]()
//        while (cursor.hasNext){
//            list.+(cursor.next())
//        }
//        list.toIterator
        new MongoMapIterator(cursor)
    }

    override protected def getPartitions: Array[Partition] = {
        val partitions:Array[Partition] = new Array[Partition](numPartitions)
        for(i:Int <- 0 until numPartitions){
            partitions((i)) = new MongoMapPartition(i,(i) * batchSize,batchSize)
        }
        partitions
    }
}

class MongoMapPartition(inde:Int,from:Int,batchSize:Int) extends Partition{


    override def index: Int = {
        inde
    }
//    override def equals(obj: scala.Any): Boolean = {
//        var tmp=false
//        if (obj.isInstanceOf[MongoMapPartition])
//            if(obj.asInstanceOf[MongoMapPartition].inde==inde)
//                tmp=true
//
//        tmp
//    }
    override def toString: String = {
        "MongoMapPartition[index=" + index + ", from=" + from + ", batchSize=" + batchSize + "]"
    }
}

class MongoMapIterator(cursor: MongoCursor[Document]) extends Iterator[Document]{

    def next(): Document ={
        cursor.next()
    }

    def hasNext(): Boolean ={
        cursor.hasNext
    }
}
