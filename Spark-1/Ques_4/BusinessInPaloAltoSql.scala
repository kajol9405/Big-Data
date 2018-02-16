    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val d1 = sc.textFile("business.csv").cache().filter(!_.isEmpty).filter(line=>line.contains("Palo Alto, CA")).cache().map(line => line.split("::")).map(line => line(0)).toDF("b_id").distinct()

    val d2 = sc.textFile("review.csv").cache().filter(!_.isEmpty).map(line => line.split("::")).map(line => (line(0), line(1), line(2), line(3))).toDF("r_id", "u_id", "b_id", "stars");

    d1.createOrReplaceTempView("business")
    d2.createOrReplaceTempView("review")

    val BusinessInPaloAlto = sqlContext.sql("SELECT  u_id, stars FROM review JOIN business ON business.b_id = review.b_id").rdd.cache()
    val BusinessInPaloAltoF = BusinessInPaloAlto.map(x => ((x.get(0).toString().replace("(","").replace(")","")+"\t"+x.get(1))))
    BusinessInPaloAltoF.repartition(1).saveAsTextFile("BusinessInPaloAltoSql");
 