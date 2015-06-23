package com.duchessfr.spark.core;

import com.duchessfr.spark.utils.Parse;
import com.duchessfr.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

  private static String pathToFile = "data/reduced-tweets.json";

  /**
   *  Load the data from the text file and return an RDD of Tweet
   */
  public JavaRDD<Tweet> loadData() {
    // create spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Inverted index")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Tweet> tweets = sc.textFile(pathToFile)
                              .map(line -> Parse.parseJsonToTweet(line));

    return tweets;
  }

  public Map<String, Iterable<Tweet>> invertedIndex() {
    JavaRDD<Tweet> tweets = loadData();

    // For each tweet t, we extract all the hashtags and create a pair (hashtag,tweet)
    JavaPairRDD<String, Tweet> pairs = tweets.flatMapToPair(tweet -> {
      List results = new ArrayList();
      List<String> hashtags = new ArrayList();
      List<String> words = Arrays.asList(tweet.getText().split(" "));

      for (String word: words) {
        if (word.startsWith("#") && word.length() > 1) {
          hashtags.add(word);
        }
      }

      for (String hashtag : hashtags) {
        Tuple2<String, Tweet> result = new Tuple2<>(hashtag, tweet);
        results.add(result);
      }

      return results;

    });

    // We use the groupBy method to group the tweets by hashtag
    JavaPairRDD<String, Iterable<Tweet>> tweetsByHashtag = pairs.groupByKey(); //expensive shuffle

    // Then return a map using the collectAsMap method on the RDD
    Map<String, Iterable<Tweet>> map = tweetsByHashtag.collectAsMap(); //even more expensive ops

    return map;

  }

}
