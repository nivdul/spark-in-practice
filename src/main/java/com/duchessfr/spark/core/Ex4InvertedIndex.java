package com.duchessfr.spark.core;

import com.duchessfr.spark.utils.Parse;
import com.duchessfr.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Map;

/**
 *  Buildind a hashtag search engine
 *
 *  The goal is to build an inverted index. An inverted is the data structure used to build search engines.
 *
 *  How does it work?
 *
 *  Assuming #spark is an hashtag that appears in tweet1, tweet3, tweet39.
 *  Our inverted index is a Map (or HashMap) that contains a (key, value) pair as (#spark, List(tweet1,tweet3, tweet39)).
 *
 */
public class Ex4InvertedIndex {

  private static String pathToFile = "data/reduced-tweets.txt";

  /**
   *  Load the data from the text file and return an RDD of Tweet
   */
  public JavaRDD<Tweet> loadData() {
    // create spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Tweet mining")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Tweet> tweets = sc.textFile(pathToFile)
                              .map(line -> Parse.parseJsonToTweet(line));

    return tweets;
  }

  public Map<String, Iterable<Tweet>> invertedIndex() {
    JavaRDD<Tweet> tweets = loadData();

    // for each tweet, extract all the hashtag and then create couples (hashtag,tweet)
    // Hint: see the flatMapToPair method

    JavaPairRDD<String, Tweet> pairs = null;

    // We want to group the tweets by hashtag
    JavaPairRDD<String, Iterable<Tweet>> tweetsByHashtag = pairs.groupByKey();

    // Then return the inverted index (= a map structure)
    Map<String, Iterable<Tweet>> map = null;

    return map;

  }

}