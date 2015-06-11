package com.duchessfr.spark.part1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


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
 *  Use the Ex0WordcountTest to implement the code.
 *
 */
public class Ex0Wordcount {

  private static String pathToFile = "data/wordcount.txt";

  /**
   *  Load the data from the text file and return an RDD of words
   */
  public JavaRDD<String> loadData() {
    // create spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Wordcount")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    // load data and create an RDD where each element will be a word
    // Hint: use the Spark context and take a look at the textfile and flatMap methods
    JavaRDD<String> words = null;

    return words;

  }

  /**
   *  Now count how much each word appears !
   */
  public JavaPairRDD<String, Integer> wordcount() {
    JavaRDD<String> words = loadData();

    // Step 1: mapper step
    // We want to attribute the number 1 to each word: so we create couples (word, 1) using the Tuple2 class.
    // Hint : look at the mapToPair method
    JavaPairRDD<String, Integer> couples = null;

    // Step 2: reducer step
    // Hint: the SPark API provides some reduce methods
    JavaPairRDD<String, Integer> result = null;

    return result;
  }

  /**
   *  Now just keep the word which appear strictly more than 4 times!
   */
  public JavaPairRDD<String, Integer> filterOnWordcount() {
    JavaPairRDD<String, Integer> wordcounts = wordcount();

    // Hint: the Spark API provides a filter method
    JavaPairRDD<String, Integer> filtered = null;

    return filtered;

  }

}
