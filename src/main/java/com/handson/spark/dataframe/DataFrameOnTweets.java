package com.handson.spark.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 *  The Spark SQL and DataFrame documentation is available on:
 *  https://spark.apache.org/docs/1.4.0/sql-programming-guide.html
 *
 *  A DataFrame is a distributed collection of data organized into named columns.
 *  The entry point before to use the DataFrame is the SQLContext class (from Spark SQL).
 *  With a SQLContext, you can create DataFrames from:
 *  - an existing RDD
 *  - a Hive table
 *  - data sources...
 *
 *  In the exercise we will create a dataframe with the content of a JSON file.
 *
 *  We want to:
 *  - print the dataframe
 *  - print the schema of the dataframe
 *  - find people who are located in Paris
 *  - find the user who tweets the more
 * 
 *  And just to recap we use a dataset with 8198 tweets,where a tweet looks like that:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 * 
 *  Use the DataFrameOnTweetsTest to implement the code.
 */
public class DataFrameOnTweets {

  private static String pathToFile = "data/reduced-tweets.json";

  /**
   *  Here the method to create the contexts (Spark and SQL) and
   *  then create the dataframe.
   *
   *  Run the test to see how looks the dataframe!
   */
  public DataFrame loadData() {

    // create spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("DataFrame")
        .set("spark.driver.allowMultipleContexts", "true")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a sql context: the SQLContext wraps the SparkContext, and is specific to Spark SQL.
    // It is the entry point in Spark SQL.
    // TODO write code here
    SQLContext sqlContext = null;

    // load the data as dataframe from the json file
    // Hint: use the sqlContext and apply the read method before loading the json file
    // TODO write code here
    DataFrame dataFrame = null;

    return dataFrame;

  }

  /**
   *  See how looks the dataframe
   */
  public void showDataFrame() {
    DataFrame dataFrame = loadData();

    // Displays the content of the DataFrame to stdout
    // TODO write code here

  }

  /**
   * Print the schema
   */
  public void printSchema() {
    DataFrame dataFrame = loadData();

    // Print the schema
    // TODO write code here
  }

  /**
   *  Find people who are located in Paris
   */
  public DataFrame filterByLocation() {
    DataFrame dataFrame = loadData();

    // Find all the persons which are located in Paris
    // TODO write code here
    DataFrame filtered = null;

    return filtered;

  }

  /**
   *  Find the user who tweets the more
   */
  public Row mostPopularTwitterer() {
    DataFrame dataFrame = loadData();

    // group the tweets by user first
    // TODO write code here
    DataFrame countByUser = null;

    // sort by descending order and take the first one
    // TODO write code here
    JavaRDD<Row> result = null;

    return result.first();

  }

}
