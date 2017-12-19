## spark join 三种方式

* broadcast join 
    > 适用于事实表和维度表之间的join，将较小的维度表作为广播变量，以便每个task中可以获取小标进行jion，缺点：围标的大小有限制：spark.sql.autoBroadcastJoinThreshold 参数默认10M 小于10M则会进行广播，否则将会异常，需适当调节参数值
* hash join
    > 将两表的key进行分区，则相同的key会被分配到同一个partition中，进行单个partition内部的join，shuffer过程不会对记录进行排序
* sort merge join
    > 为hash join的升级版，在hash的repartition过程中对记录进行排序，并在排序后的数据集中按照顺序查找并对两侧数据进行join
    
    ![示意图](https://github.com/duguyiren3476/spark_tools/blob/master/data/sort-merge.gif)

[参考](http://sharkdtu.com/posts/spark-sql-join.html)