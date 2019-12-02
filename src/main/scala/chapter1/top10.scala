package chapter1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object top10 {
  def main(args: Array[String]): Unit = {
    /**
     * 电影流行度分析：所有电影中平均得分最高（口碑最好）的电影及观看人数最高的电影（流行度最高）
     * "ratings.dat"：UserID::MovieID::Rating::Timestamp
     * 得分最高的Top10电影实现思路：如果想算总的评分的话一般肯定需要reduceByKey操作或者aggregateByKey操作
     *   第一步：把数据变成Key-Value，大家想一下在这里什么是Key，什么是Value。把MovieID设置成为Key，把Rating设置为Value；
     *   第二步：通过reduceByKey操作或者aggregateByKey实现聚合，然后呢？
     *   第三步：排序，如何做？进行Key和Value的交换
     *
     *  注意：
     *   1，转换数据格式的时候一般都会使用map操作，有时候转换可能特别复杂，需要在map方法中调用第三方jar或者so库；
     *   2，RDD从文件中提取的数据成员默认都是String方式，需要根据实际需要进行转换格式；
     *   3，RDD如果要重复使用，一般都会进行Cache
     *   4， 重磅注意事项，RDD的cache操作之后不能直接在跟其他的算子操作，否则在一些版本中cache不生效
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    var dataPath = "data/moviedata/medium/"  //数据存放的目录；
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")


    val movieInfo = moviesRDD.map(_.split("::")).map(x => (x(0),x(1))) // (movieId, moveName)
    val ratingInfo = ratingsRDD.map(_.split("::")).map(x => (x(0),x(1),x(2)))//(userId, movieId,movieSocre)
    val movidAndRating = ratingInfo.map(x => (x._2,(x._3.toDouble,1)))
        .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
        .map(x => (x._2._1.toDouble/x._2._2,x._1))
        .sortByKey(false)
        .take(10)
        .foreach(println)


    println("所有电影中平均得分最高（口碑最好）的电影:")
    val ratings= ratingsRDD.map(_.split("::")).map(x => (x(0), x(1), x(2))).cache()  //格式化出电影ID和评分
    ratings.map(x => (x._2, (x._3.toDouble, 1)))  //格式化成为Key-Value的方式
      .reduceByKey((x, y) => (x._1 + y._1,x._2 + y._2)) //对Value进行reduce操作，分别得出每部电影的总的评分和总的点评人数
      .map(x => (x._2._1.toDouble / x._2._2, x._1))  //求出电影平均分
      .sortByKey(false) //降序排序
      .take(10) //取Top10
      .foreach(println) //打印到控制台
   println("---------------------------------------------")
   val avg = ratingInfo.map(x => (x._2,(x._3.toDouble,1)))
     .reduceByKey((x,y) => (x._1+y._1,x._2+y._2)).map(x => (x._1,x._2._1.toDouble / x._2._2)).join(movieInfo)
     .map(x => (x._2._1,x._2._2)).sortByKey(false).take(10)
     .foreach(record => println(record._2+"的score: "+record._1))


    val userInfo = usersRDD.map(_.split("::")).map(x => (x(0),x(1)))
    val userRating = ratingInfo.map(x => (x._1,(x._1,x._2,x._3))).join(userInfo)
//    userRating.take(10).foreach(println)
    val maleFilterRating = userRating.filter(x => x._2._2.equals("M")).map(x => x._2._1)
    val fmaleFilterRating = userRating.filter(x => x._2._2.equals("F")).map(x => x._2._1)
    println("所有电影中男性最喜欢的电影:")
    maleFilterRating.map(x => (x._2,(x._3.toDouble,1))).reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
      .map(x => (x._1,x._2._1.toDouble / x._2._2)).join(movieInfo)
      .map(x => (x._2._1,x._2._2)).sortByKey(false).take(10)
      .foreach(record => println(record._2+"的score: "+record._1))
    println("所有电影中性最喜欢的电影:")
    fmaleFilterRating.map(x => (x._2,(x._3.toDouble,1))).reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
      .map(x => (x._1,x._2._1.toDouble / x._2._2)).join(movieInfo)
      .map(x => (x._2._1,x._2._2)).sortByKey(false).take(10)
      .foreach(record => println(record._2+"的score: "+record._1))
    sc.stop()
  }
}
