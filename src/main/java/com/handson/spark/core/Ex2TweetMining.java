package com.handson.spark.core;

import com.handson.spark.utils.Parse;
import com.handson.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  We still use the dataset with the 8198 reduced tweets. Here an example of a tweet:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 *
 *  We want to make some computations on the tweets:
 *  - Find all the persons mentioned on tweets
 *  - Count how many times each person is mentioned
 *  - Find the 10 most mentioned persons by descending order
 *
 *  Use the Ex2TweetMiningTest to implement the code.
 */
public class Ex2TweetMining {

  private static String pathToFile = "data/reduced-tweets.json";

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  public JavaRDD<Tweet> loadData() {
    // create spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Tweet mining")
        .set("spark.driver.allowMultipleContexts", "true")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    // load the data and create an RDD of Tweet
    JavaRDD<Tweet> tweets = sc.textFile(pathToFile)
                              .map(line -> Parse.parseJsonToTweet(line));

    return tweets;
  }

  /**
   *  Find all the persons mentioned on tweets (case sensitive)
   */
  public JavaRDD<String> mentionOnTweet() {
    JavaRDD<Tweet> tweets = loadData();

    // You want to return an RDD with the mentions
    // Hint: think about separating the word in the text field and then find the mentions
    // TODO write code here
    JavaRDD<String> mentions = null;

    return mentions;

  }
  /**
   *  Count how many times each person is mentioned
   */
  public JavaPairRDD<String, Integer> countMentions() {
    JavaRDD<String> mentions = mentionOnTweet();

    // Hint: think about what you did in the wordcount example
    // TODO write code here
    JavaPairRDD<String, Integer> mentionCount = null;

    return mentionCount;
  }

  /**
   *  Find the 10 most mentioned persons by descending order
   */
  public List<Tuple2<Integer, String>> top10mentions() {
    JavaPairRDD<String, Integer> counts = countMentions();

    // Hint: take a look at the sorting and take methods
    // TODO write code here
    List<Tuple2<Integer, String>> mostMentioned = null;

    return mostMentioned;
  }

}
