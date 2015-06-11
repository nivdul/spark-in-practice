package com.duchessfr.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

/**
 *  The Spark Streaming documentation is available on:
 *  http://spark.apache.org/docs/latest/streaming-programming-guide.html
 *
 *  Spark Streaming is an extension of the core Spark API that enables scalable,
 *  high-throughput, fault-tolerant stream processing of live data streams.
 *  Spark Streaming receives live input data streams and divides the data into batches,
 *  which are then processed by the Spark engine to generate the final stream of results in batches.
 *  Spark Streaming provides a high-level abstraction called discretized stream or DStream,
 *  which represents a continuous stream of data.
 *
 *  In this exercise we will:
 *  - Print the status of each tweet
 *  - Find the 10 most popular Hashtag
 *
 */
public class PlayWithSparkStreaming {

  /**
   *  Load the data using TwitterUtils: we obtain a DStream of tweets
   *
   *  More about TwitterUtils:
   *  https://spark.apache.org/docs/0.9.0/api/external/twitter/index.html#org.apache.spark.streaming.twitter.TwitterUtils$
   */

  /**
   *  Print the status's text of each status:
   *  - a status is
   */
  private static void tweetPrint(JavaDStream<Status> tweetsStream) {
    JavaDStream<String> status = tweetsStream.map(tweetStatus -> tweetStatus.getText());

    status.print();

  }

  public static void main(String[] args) {
    // create the spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Play with Spark Streaming")
        .setMaster("local[*]");

    // create a java streaming context and define the window
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

    System.out.println("Initializing Twitter stream...");

    // create a DStream (sequence of RDD):
    // - the Status class contains all information of a tweet
    // See http://twitter4j.org/javadoc/twitter4j/Status.html
    JavaDStream<Status> tweetsStream = TwitterUtils.createStream(jssc, StreamUtils.getAuth());

    tweetPrint(tweetsStream);

    // Start the context
    jssc.start();
    jssc.awaitTermination();
  }

}
