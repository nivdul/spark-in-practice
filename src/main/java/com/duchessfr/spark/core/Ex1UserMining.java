package com.duchessfr.spark.core;

import com.duchessfr.spark.utils.Parse;
import com.duchessfr.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  Now we use another dataset (with 8198 tweets). The data are reduced tweets as the example below:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 *
 *  We want to make some computations on the users:
 *  - find all the tweets by user
 *  - find how many tweets each user has
 *
 */
public class Ex1UserMining {

  private static String pathToFile = "data/reduced-tweets.json";

  /**
   *  Load the data from the text file and return an RDD of Tweet
   */
  public JavaRDD<Tweet> loadData() {
    // create spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("User mining")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Tweet> tweets = sc.textFile(pathToFile)
                              .map(line -> Parse.parseJsonToTweet(line));

    return tweets;
  }

  /**
   *   Return for each user all his tweets
   */
  public JavaPairRDD<String, Iterable<Tweet>> tweetsByUser() {
    JavaRDD<Tweet> tweets = loadData();

    // TODO write code here
    // Hint: take a look at the groupBy method
    JavaPairRDD<String, Iterable<Tweet>> tweetsByUser = null;

    return tweetsByUser;

  }

  /**
   *  Compute the number of tweets by user
   */
  public JavaPairRDD<String, Integer> tweetByUserNumber() {
    JavaRDD<Tweet> tweets = loadData();

    // TODO write code here
    // Hint: think about the wordcount example
    JavaPairRDD<String, Integer> count = null;

    return count;
  }

}
