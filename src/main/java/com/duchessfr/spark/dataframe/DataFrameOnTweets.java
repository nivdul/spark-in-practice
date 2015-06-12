package com.duchessfr.spark.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;

/**
 *  The Spark SQL and DataFrame documentation is available on:
 *  https://spark.apache.org/docs/1.4.0/sql-programming-guide.html
 *
 *  A DataFrame is a distributed collection of data organized into named columns.
 *  The entry point before to use the DataFrame is the SQLContext class (from SPark SQL).
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
        .setAppName("User mining")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);
    // create a sql context
    SQLContext sqlContext = new SQLContext(sc);

    DataFrame dataFrame = sqlContext.read().json(pathToFile);

    return dataFrame;

  }

  /**
   *  See how looks the dataframe
   */
  public void showDataFrame() {
    DataFrame dataFrame = loadData();

    // Displays the content of the DataFrame to stdout
    dataFrame.show();
  }

  /**
   * Print the schema
   */
  public void printSchema() {
    DataFrame dataFrame = loadData();

    // Print the schema
    dataFrame.printSchema();
  }

  public DataFrame filterByLocation() {
    DataFrame dataFrame = loadData();

    // Select all the persons which are located in Paris
    DataFrame filtered = dataFrame.filter(dataFrame.col("place").equalTo("Paris")).toDF();

    return filtered;

  }

  public void mostTweeter() {
    DataFrame dataFrame = loadData();

    // group the tweets by user first
    DataFrame countByUser = dataFrame.groupBy(dataFrame.col("user")).count();


    System.out.println(countByUser.javaRDD().first());

    // TODO
    countByUser.javaRDD().sortBy(new Function<Row, Object>() {

      @Override
      public Object call(Row row) throws Exception {

        return null;
      }
    }, false, 1);

  }


}
