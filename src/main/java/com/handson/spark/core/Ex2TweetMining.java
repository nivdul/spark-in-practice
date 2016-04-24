package com.handson.spark.core;

import com.handson.spark.utils.Parse;
import com.handson.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  Now we use a dataset with 8198 tweets. Here an example of a tweet:
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
 */
public class Ex2TweetMining {

  private static String pathToFile = "data/reduced-tweets.json";

  /**
   *  Load the data from the text file and return an RDD of Tweet
   */
  public JavaRDD<Tweet> loadData() {
    // create spark configuration and spark context
    SparkConf conf = new SparkConf()
                            .setAppName("Tweet mining")
                            .set("spark.driver.allowMultipleContexts", "true")
                            .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Tweet> tweets = sc.textFile(pathToFile)
                              .map(line -> Parse.parseJsonToTweet(line));

    return tweets;
  }

  /**
   *  Find all the persons mentioned on tweets
   */
  public JavaRDD<String> mentionOnTweet() {
    JavaRDD<Tweet> tweets = loadData();

    JavaRDD<String> mentions = tweets.flatMap(tweet -> Arrays.asList(tweet.getText().split(" ")))
                                     .filter(word -> word.startsWith("@") && word.length() > 1);

    System.out.println("mentions.count() " + mentions.count());
    return mentions;

  }

  /**
   *  Count how many times each person is mentioned
   */
  public JavaPairRDD<String, Integer> countMentions() {
    JavaRDD<String> mentions = mentionOnTweet();

    JavaPairRDD<String, Integer> mentionCount = mentions.mapToPair(mention -> new Tuple2<>(mention, 1))
                                                         .reduceByKey((x, y) -> x + y);

    return mentionCount;
  }

  /**
   *  Find the 10 most mentioned persons by descending order
   */
  public List<Tuple2<Integer, String>> top10mentions() {
    JavaPairRDD<String, Integer> counts = countMentions();

    List<Tuple2<Integer, String>> mostMentioned = counts.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                                                        .sortByKey(false)
                                                        .take(10);

    return mostMentioned;
  }

}
