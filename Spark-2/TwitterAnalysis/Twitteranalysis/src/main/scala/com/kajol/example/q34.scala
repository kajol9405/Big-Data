package com.kajol.example

import java.io.File
import java.util.Properties

import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import Sentiment.Sentiment
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.convert.wrapAll._

/**
  * Collect at least the specified number of tweets into json text files.
  */
object q34 {
  private var no_of_tweets_received = 0L
  private var partNum = 0
  private var gson = new Gson()

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  def main(args: Array[String]) {
   
    if (args.length < 1) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        "<outputDirectory>")
      System.exit(1)
    }
    
    val outDirectory = new File(args(0))
    if (outDirectory.exists()) {
      outDirectory.delete()
    }
    outDirectory.mkdirs()

    
    System.setProperty("twitter4j.oauth.consumerKey", "YbMNDmWJRDyU9Uj3eOsslWIj0")
    System.setProperty("twitter4j.oauth.consumerSecret", "8wcAhIyBfeqgDsMcdK4hJjlGpyNG9je8TcO9a0i9BEgVXWRegj")
    System.setProperty("twitter4j.oauth.accessToken", "1019726347-vwTHII4PN3JFSmIDsyQ1zdU4xP20Y4YD9gkiNrr")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "oz79sqcTkS1rpuxkDYQ9g3hGiTXZNv1WTxUoT2UDHRies")

    
    println("Initializing Streaming Spark Context...")
    val sparkConf = new SparkConf().setAppName("#twitterSentiment").setMaster("local[2]")
    val sc = new StreamingContext(sparkConf, Seconds(30))


    val filtered_tweets = Array("#trump", "#obama")
    val stream = TwitterUtils.createStream(sc, None, filtered_tweets)
    val Tweets_english = stream.filter(_.getLang() == "en")
    Tweets_english.saveAsTextFiles(args(0) + "/tweets_")
    

    Tweets_english.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(1)
        val stringSentiments = outputRDD.map {
          string =>
            val sentiment = Vector(string.getText, sentimentAnalysis(string.getText))
            sentiment
        }

        stringSentiments.saveAsTextFile(args(0) + "/tweets_" + time.milliseconds.toString)
        no_of_tweets_received += count
        if (no_of_tweets_received > 100) {
          System.exit(0)
        }
      }
    })

    sc.start()
    sc.awaitTermination()
  }

  
  
  def sentimentAnalysis(text: String): Sentiment = {
    val (_, sentiment) = sentimentsAnalysis(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
  }
  
  

  def sentimentsAnalysis(text: String): List[(String, Sentiment)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString,Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }
  
  
}
