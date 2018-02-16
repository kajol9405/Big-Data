    val d1 = sc.textFile("business.csv").cache()

    val filteredbus = d1.filter(!_.isEmpty).map(line => line.split("::")).map(line => (line(0),line)).distinct()//(b_id, (b_id,add,cat))


    val d2 = sc.textFile("review.csv").cache()

    val filteredrev = d2.filter(!_.isEmpty).map(line => line.split("::")).map(line => line(2))

    val count = filteredrev.map(x => (x,1)).reduceByKey(_+_).map(x => (x._1 ,x._2)).sortBy(_._2,false) //(b_id, count of most reviewed business)

    val joinTable = count.join(filteredbus).values

    val topTenmostreviewedbusiness = joinTable.map(x => (x._2.mkString(" "),x._1)).distinct().sortBy(_._2,false).take(10)

    val topTenmostreviewedbusinessF = topTenmostreviewedbusiness.map(x => (x._1 + "\t" +x._2))

    sc.parallelize(topTenmostreviewedbusinessF).repartition(1).saveAsTextFile("topTenMostreviewedBusinessSpark")