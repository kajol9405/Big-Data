import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

val data = sc.textFile("itemusermat")
val dataMap = data.map(s => Vectors.dense(s.split(' ').drop(1).map(_.toDouble))).cache()

val movies = sc.textFile("movies.dat")
val parsedmovies = movies.map(x => (x.split("::"))).map( x => ( x(0),(x(1)+", "+x(2))))

val numOfClusters = 10
val iterations = 100
val kMeansModel = KMeans.train(dataMap, numOfClusters, iterations)

val predictions = data.map{x =>(x.split(' ')(0), kMeansModel.predict(Vectors.dense(x.split(' ').drop(1).map(_.toDouble))))}

val joinData = predictions.join(parsedmovies)
val predictionsGrouped = joinData.map(x => (x._2._1, (x._1, x._2._2))).groupByKey()

val result = predictionsGrouped.map(x => (x._1,x._2.toList))

val finalResults = result.map(x => (x._1,x._2.take(5)))

println("")
finalResults.foreach( x => println("Cluster: " + x._1 + " \n " + x._2.mkString("\n")))
