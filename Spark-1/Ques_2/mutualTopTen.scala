
    val rdd = sc.textFile("test.txt").cache()
    val userRdd = sc.textFile("userdata.txt").cache()
    var userToFriendsRdd = rdd.filter(y => (y.split("\t").size > 1)).map(y => y.split("\t")).map(tokens => (tokens(0), tokens(1).split(",").filter(!_.isEmpty).toList))
    val userDetails = userRdd.map(y => y.split(",")).map(x => (x(0),x(1),x(2),x(3)))

    def createPair(y: String): Array[(String, String)] = {
      val splits = y.split("\t")
      val kuid = splits(0)
      if (splits.size > 1) {
        splits(1).split(",").map {
          segment => {
            if (segment < kuid) {
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
    val friendList = userToFriendsRdd.map(x => (x._1, x))

    val firstUserFriendList = pairs.join(friendList).cache()
    val secondUserFriendList = reversedPairs.join(friendList).cache()
    val firstUserFriends = firstUserFriendList.map(x => (x._1, x._2._1, x._2._2._2))
    val secondUserFriends = secondUserFriendList.map(x => (x._2._1, x._1, x._2._2._2))

    val firstUsersecondUserFriends = firstUserFriends.map {
      case (key1, key2, value) => ((key1, key2), value)
    }.join(
        secondUserFriends.map {
          case (key1, key2, value) => ((key1, key2), value)
        })

    val mutualFriends = firstUsersecondUserFriends.map(x => (x._1,x._2._1.intersect(x._2._2).size))
    val topMFrnds = mutualFriends.sortBy(_._2,false)
    val topMFrnds1 =topMFrnds.map(y => (y._1._1,y._1._2,y._2))
    val users = topMFrnds.map(x => (x._1._1,x._1._2))
    val usersReversed = topMFrnds.map(x => (x._1._2,x._1._1))
    val userDetailsJoinData = userDetails.map(x => (x._1,x))
    val firstUserDetailsJoin = users.join(userDetailsJoinData)
    val firstUserDetails = firstUserDetailsJoin.map(x => (x._1, x._2._1, (x._2._2._2,x._2._2._3,x._2._2._4).toString()))
    val secondUserDetailsJoin = usersReversed.join(userDetailsJoinData)
    val secondUserDetails = secondUserDetailsJoin.map(x => (x._2._1, x._1, (x._2._2._2,x._2._2._3,x._2._2._4).toString()))

    val firstUsersecondUserDetailsJoin = firstUserDetails.map {
      case (key1, key2, value) => ((key1, key2), value)
    }.join(
        secondUserDetails.map {
          case (key1, key2, value) => ((key1, key2), value)
        })

    val tp = firstUsersecondUserDetailsJoin.map(x => (x._1._1,x._1._2,x))

    val joinedTable = topMFrnds1.map {
      case (key1, key2, value) => ((key1, key2), value)
    }.join(
        tp.map {
          case (key1, key2, value) => ((key1, key2), value)
        })

    val finalAnswers = joinedTable.map(d => (d._1._1,d._1._2,d._2._1,d._2._2._2._1,d._2._2._2._2))
    val srt= finalAnswers.sortBy(_._3,false).zipWithIndex.filter{case (_, idx) => idx < 11}.keys
    val finalOutput= srt.map(x=> (x._3.toString + "\t" + x._4.toString().replace(",","\t").replace("(","").replace(")","") + "\t\t\t" + x._5.toString().replace(",","\t").replace("(","").replace(")","")))

    finalOutput.repartition(1).saveAsTextFile("Top10Mutual");

  