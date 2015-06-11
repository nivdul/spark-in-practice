package com.duchessfr.spark.core;

import com.duchessfr.spark.utils.Parse;
import com.duchessfr.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

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

  private static String pathToFile = "data/reduced-tweets.txt";

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

    JavaPairRDD<String, Iterable<Tweet>> tweetsByUser = tweets.groupBy(tweet -> tweet.getUser());

    return tweetsByUser;

  }

  /**
   *  Compute the number of tweets by user
   */
  public JavaPairRDD<String, Integer> tweetByUserNumber() {
    JavaRDD<Tweet> tweets = loadData();

    JavaPairRDD<String, Integer> count = tweets.mapToPair(t -> new Tuple2<>(t.getUser(), 1))
                                               .reduceByKey((x, y) -> x + y);

    return count;
  }

}
