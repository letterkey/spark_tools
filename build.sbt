name := "spark_tools"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.4.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.34"

libraryDependencies += "org.mongodb" % "bson" % "2.11.3"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1"

libraryDependencies += "org.mongodb" % "mongo-java-driver" % "2.11.4"

libraryDependencies += "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.4.0"

libraryDependencies += "redis.clients" % "jedis" % "2.7.2"

libraryDependencies += "org.json" % "json" % "20141113"

libraryDependencies += "com.google.code.gson" % "gson" % "2.3"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"