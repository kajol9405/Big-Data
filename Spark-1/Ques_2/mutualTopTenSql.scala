
val sContext = new org.apache.spark.sql.SQLContext(sc)

    sContext.setConf("spark.sql.tungsten.enabled", "false")

    import sContext.implicits._

    val rdd = spark.sparkContext.textFile("test.txt").cache()
    var userToFriendsRdd = rdd.filter(y => (y.split("\t").size > 1)).map(y => y.split("\t")).map(tokens => (tokens(0),tokens(1).split(",").filter(!_.isEmpty).toList))

    def createPair(y: String): Array[(String, String)] = {
      val splits = y.split("\t")
      val kuid = splits(0)
      if (splits.size > 1) {
        splits(1).split(",").map {
          segment => {
            if (segment.toLong < kuid.toLong) {
              (segment, kuid)
            } else {
              (kuid, segment)
            }
          }
        }
      } else {
        Array(null)
      }
    }

    val pair = rdd.flatMap { y => createPair(y) }.distinct()
    val pairs = pair.filter(x => x != null)
    val reversedPairs = pairs.map(x => (x._2, x._1))
    val pairsDF = pairs.toDF("firstUser","secondUser")
    val userFriends = userToFriendsRdd.toDF("user","friends")

    pairsDF.registerTempTable("pairs")

    userFriends.registerTempTable("friends")

    val firstUserFriends = sContext.sql("select firstUser,secondUser,friends from pairs join friends on pairs.firstUser = friends.user")
    val userMFriends = sContext.sql("select firstUser,secondUser,friends from pairs join friends on pairs.secondUser = friends.user")
    val firstUserfList = firstUserFriends.toDF("firstUser","secondUser","f1")
    val secondUserfList = userMFriends.toDF("firstUser","secondUser","f2")

    firstUserfList.registerTempTable("firstUserfList")

    secondUserfList.registerTempTable("secondUserfList")

    val mutual = sContext.sql("select firstUserfList.firstUser,firstUserfList.secondUser,f1,f2 from firstUserfList join secondUserfList on firstUserfList.firstUser = secondUserfList.firstUser and firstUserfList.secondUser = secondUserfList.secondUser")

    val mutualDF = mutual.toDF("firstUser","secondUser","friend1List","friend2List")

    mutualDF.registerTempTable("mutual")

    spark.udf.register("array_intersect",(xs: Seq[String], ys: Seq[String]) => xs.intersect(ys).size)

    val mFrnds = sContext.sql("select firstUser,secondUser,array_intersect(friend1List,friend2List) as count from mutual order by count desc limit 10")

    val mFrndsDF = mFrnds.toDF("firstUser","secondUser","count")

    mFrndsDF.registerTempTable("mFrnds")

    val userRdd = spark.sparkContext.textFile("userdata.txt")
    val userDetails = userRdd.map(y => y.split(",")).map(x => (x(0),x(1),x(2),x(3)))
    val userData = userDetails.toDF("id","firstname","lastname","address")

    userData.registerTempTable("userData")

    val firstUserData = sContext.sql("select firstUser,secondUser,count,firstname,lastname,address from mFrnds join userData on mFrnds.firstUser = userData.id")
    val secondUserData = sContext.sql("select firstUser,secondUser,count,firstname,lastname,address from mFrnds join userData on mFrnds.secondUser = userData.id")
    val fTab = firstUserData.toDF("firstUser","secondUser","count","firstname","lastname","address")
    val sTab = secondUserData.toDF("firstUser","secondUser","count","firstname","lastname","address")

    fTab.registerTempTable("fTab")
    sTab.registerTempTable("sTab")

    val ans = sContext.sql("select fTab.firstUser,fTab.secondUser,fTab.count,fTab.firstname,fTab.lastname,fTab.address,sTab.firstname,sTab.lastname,sTab.address from fTab,sTab where fTab.firstUser = sTab.firstUser and fTab.secondUser = sTab.secondUser")
    val res = ans.rdd.map(x => (x.get(2) + "\t" + x.get(3) + "\t" + x.get(4) + "\t" + x.get(5)+ "\t" + x.get(6) + "\t" + x.get(7) + "\t" + x.get(8)))

    res.repartition(1).saveAsTextFile("MutuaTop10FrndsSQL")

    spark.stop()