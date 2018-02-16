val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    val d1 = sc.textFile("business.csv").cache().filter(!_.isEmpty).map(line => line.split("::")).map(line => (line(0),line(1),line(2))).distinct().toDF("b_id","add","cat")

    val d2 = sc.textFile("review.csv").cache().filter(!_.isEmpty).map(line => line.split("::")).map(line => ( line(1), line(2))).toDF( "u_id", "b_id");

    d1.createOrReplaceTempView("business")
    d2.createOrReplaceTempView("review")

    val topTenmostreviewedbusiness = sqlContext.sql("SELECT business.b_id, add, cat, count(review.u_id)  FROM business, review WHERE business.b_id = review.b_id group by business.b_id, add, cat order by count(review.u_id) DESC LIMIT 10").rdd.cache()

    val topTenmostreviewedbusinessF = topTenmostreviewedbusiness.map(x => (x.get(0) + "\t" + x.get(1) + "\t" + x.get(2) + "\t" + x.get(3)))

    topTenmostreviewedbusinessF.repartition(1).saveAsTextFile("topTenMostreviewedBusinesssql");
  
