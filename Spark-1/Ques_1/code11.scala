      val socRDD = sc.textFile("test.txt").cache()

      var SplittedRDD = socRDD.filter(line => (line.split("\t").size > 1)).map(line => line.split("\t")).map(tokens => (tokens(0),tokens(1).split(",").filter(!_.isEmpty).toList))

      def generatePair(line: String): Array[(String,List[String]  )] = {
        val splits = line.split("\t")
        val kuid = splits(0)
        if (splits.size > 1) {
          val friends = splits(1).split(",").toList
          splits(1).split(",").map {
            segment => {
              if (segment.toLong < kuid.toLong) {
                ((segment, kuid).toString(),friends)
              } else {
                ((kuid, segment).toString(),friends)
              }
            }
          }
        } else {
          Array(null)
        }
      }

      val pair = socRDD.flatMap { line => generatePair(line) }.distinct()

      val pairs = pair.filter(x => x != null)

      val data = pairs.map(x =>(x._1,x._2))

      val groupedData = data.groupByKey

      val intersection = groupedData.map(x => (x._1,x._2.flatten.groupBy(identity).collect { case (x, List(_,_,_*)) => x }.size))

      val mutualFriendsOfUsers = intersection.map(x => x._1.toString().replace("(","").replace(")","")+"\t"+x._2)

      mutualFriendsOfUsers.repartition(1).saveAsTextFile("mutualFriendsCountSparkContext")

    