val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val socfileRdd = sc.textFile("test.txt").cache()

    var SplittedRdd = socfileRdd.filter(line => (line.split("\t").size > 1)).map(line => line.split("\t")).map(tokens => (tokens(0),tokens(1).split(",").filter(!_.isEmpty).toList))


    def generateallFrnds(line: String): Array[(String, String)] = {
      val splits = line.split("\t")
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

    val firstPairofFrnds = socfileRdd.flatMap { line => generateallFrnds(line) }.distinct()
    val allFrnds = firstPairofFrnds.filter(x => x != null)
    val reversedallFrnds = allFrnds.map(x => (x._2, x._1))
    val allFrndsDF = allFrnds.toDF("firstUser","secondUser")
    val FriendsList = SplittedRdd.toDF("user","friends")

    allFrndsDF.registerTempTable("allFrnds")

    FriendsList.registerTempTable("friends")

    val firstUserFriends = sqlContext.sql("select firstUser,secondUser,friends from allFrnds join friends on allFrnds.firstUser = friends.user")
    val userMFriends = sqlContext.sql("select firstUser,secondUser,friends from allFrnds join friends on allFrnds.secondUser = friends.user")
    val firstUserFriendsList = firstUserFriends.toDF("firstUser","secondUser","f1")
    val secondUserFriendsList = userMFriends.toDF("firstUser","secondUser","f2")

    firstUserFriendsList.registerTempTable("firstUserFriendsList")

    secondUserFriendsList.registerTempTable("secondUserFriendsList")

    val mutual = sqlContext.sql("select firstUserFriendsList.firstUser,firstUserFriendsList.secondUser,f1,f2 from firstUserFriendsList join secondUserFriendsList on firstUserFriendsList.firstUser = secondUserFriendsList.firstUser and firstUserFriendsList.secondUser = secondUserFriendsList.secondUser")
    val mutualDF = mutual.toDF("firstUser","secondUser","friend1List","friend2List")

    mutualDF.registerTempTable("mutual")

    spark.udf.register("array_intersect",(xs: Seq[String], ys: Seq[String]) => xs.intersect(ys).size)

    val mf = sqlContext.sql("select firstUser,secondUser,array_intersect(friend1List,friend2List) as count from mutual")
    val mRDD= mf.rdd.map(x => ((x.get(0),x.get(1)).toString(),x.get(2)))
    val finalMF = mRDD.map(x => x._1.toString().replace("(","").replace(")","")+"\t"+x._2)

    finalMF.repartition(1).saveAsTextFile("mutualFriendsCountSql")

