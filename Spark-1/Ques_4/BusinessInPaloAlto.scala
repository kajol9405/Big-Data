  val buz	= sc.textFile("business.csv");

  val rev	= sc.textFile("review.csv");

  val filteredRev	= rev.map(line=>line.split("::")).map(line=>(line(2),line)); // b_id, review data

  val filteredBuz = buz.filter(line=>line.contains("Palo Alto, CA")).map(line=>line.split("::")).map(line=>(line(0),line(0))).distinct();// b_id, b_id

 // val joinedTable = filteredBuz.leftOuterJoin()
  val joinedTable	= filteredBuz.join(filteredRev).values.values.map(line =>(line(1), line(3)));

  val joinedTableF = joinedTable.map(x => ((x._1.toString().replace("(","").replace(")","")+"\t"+x._2)))

  joinedTableF.repartition(1).saveAsTextFile("BusinessinPaloAltoSpark");

