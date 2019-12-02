object Movie_Users_Analyzer_RDDCase {
  def main(args: Array[String]): Unit = {
    print("aaa")
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    var masterUrl = "local[4]" //默认程序运行在本地Local模式中，主要学习和测试；
//    var dataPath = "data/moviedata/medium/"  //数据存放的目录；
//
//    /**
//     * 当我们把程序打包运行在集群上的时候一般都会传入集群的URL信息，在这里我们假设如果传入
//     * 参数的话，第一个参数只传入Spark集群的URL第二个参数传入的是数据的地址信息；
//     */
//    if(args.length > 0) {
//      masterUrl = args(0)
//    } else if (args.length > 1) {
//      dataPath = args(1)
//    }
//
//    /**
//     * 创建Spark集群上下文sc，在sc中可以进行各种依赖和参数的设置等，大家可以通过SparkSubmit脚本的help去看设置信息；
//     */
//    val sc = new SparkContext(new SparkConf().setMaster(masterUrl).setAppName("Movie_Users_Analyzer_RDD"))
//
//    /**
//     * 读取数据，用什么方式读取数据呢？在这里是使用RDD!
//     */
//    val usersRDD = sc.textFile(dataPath + "users.dat")
//    val moviesRDD = sc.textFile(dataPath + "movies.dat")
//    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
//    //    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
//    val ratingsRDD = sc.textFile("data/moviedata/large/" + "ratings.dat")
//
//    /**
//     * 电影点评系统用户行为分析之一：分析具体某部电影观看的用户信息，例如电影ID为1193的
//     *   用户信息（用户的ID、Age、Gender、Occupation）
//     */
//    val usersBasic = usersRDD.map(_.split("::")).map{user => (//UserID::Gender::Age::OccupationID
//      user(3),
//      (user(0),user(1),user(2))
//    )
//    }
//    val occupations = occupationsRDD.map(_.split("::")).map(job => (job(0), job(1)))
//
//    val userInformation = usersBasic.join(occupations)
//
//    userInformation.cache()
//
//    //    for (elem <- userInformation.collect()) {
//    //      println(elem)
//    //    }
//
//
//    val targetMovie = ratingsRDD.map(_.split("::")).map(x => (x(0), x(1))).filter(_._2.equals("1193"))
//    //(11,((4882,M,45),lawyer))
//    val targetUsers = userInformation.map(x => (x._2._1._1, x._2))
//
//    val userInformationForSpecificMovie = targetMovie.join(targetUsers)
//    for (elem <- userInformationForSpecificMovie.collect()) {
//      println(elem)
//    }
  }
}
