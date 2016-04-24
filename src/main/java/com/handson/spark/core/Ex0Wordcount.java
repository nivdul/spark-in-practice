package com.handson.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  Here the goal is to count how much each word appears in a file and make some operation on the result.
 *  We use the mapreduce pattern to do this:
 *
 *  step 1, the mapper:
 *  - we attribute 1 to each word. And we obtain then couples (word, 1), where word is the key.
 *
 *  step 2, the reducer:
 *  - for each key (=word), the values are added and we will obtain the total amount.
 *
 */
public class Ex0Wordcount {

  private static String pathToFile = "data/wordcount.txt";

  /**
   *  Load the data from the text file and return an RDD of words
   */
  public JavaRDD<String> loadData() {
    // create spark configuration and spark context: the Spark context is the entry point in Spark.
    // It represents the connexion to Spark and it is the place where you can configure the common properties
    // like the app name, the master url, memories allocation...
    SparkConf conf = new SparkConf()
        .setAppName("Wordcount")
        .set("spark.driver.allowMultipleContexts", "true")
        .setMaster("local[*]"); // here local mode. And * means you will use as much as you have cores.

    JavaSparkContext sc = new JavaSparkContext(conf);

    // load data and create an RDD where each element will be a word
    // Here the flatMap method is used to separate the word in each line using the space separator
    // In this way it returns an RDD where each "element" is a word
    JavaRDD<String> words = sc.textFile(pathToFile).flatMap(line -> Arrays.asList(line.split(" ")));

    return words;

  }

  /**
   *  Now count how much each word appears !
   */
  public JavaPairRDD<String, Integer> wordcount() {
    JavaRDD<String> words = loadData();

    // Step 1: mapper step
    JavaPairRDD<String, Integer> couples = words.mapToPair(word -> new Tuple2<>(word, 1));

    // Step 2: reducer step
    JavaPairRDD<String, Integer> result = couples.reduceByKey((a, b) -> a + b);

    return result;

  }

  /**
   *  Now just keep the word which appear strictly more than 4 times!
   */
  public JavaPairRDD<String, Integer> filterOnWordcount() {
    JavaPairRDD<String, Integer> wordcounts = wordcount();

    JavaPairRDD<String, Integer> filtered = wordcounts.filter(couple -> couple._2() > 4);

    return filtered;

  }

}
