package com.duchessfr.spark.core;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

/**
 * Here are the tests to help you to implement the Ex0Wordcount
 */
public class Ex0WordcountTest {

  private Ex0Wordcount ex0Wordcount;

  @Before
  public void init() {
    ex0Wordcount = new Ex0Wordcount();
  }

  @Test
  public void loadData() {
    // run
    JavaRDD<String> words = ex0Wordcount.loadData();

    // assert
    Assert.assertEquals(809, words.count());
  }

  @Test
  public void wordcount() {
    // run
    JavaPairRDD<String, Integer> couples = ex0Wordcount.wordcount();

    // assert
    Assert.assertEquals(381, couples.count());
    Tuple2<String, Integer> theCouple = couples.filter(word -> "the".equals(word._1())).first();
    Assert.assertEquals(38, theCouple._2().intValue());
    Tuple2<String, Integer> generallyCouple = couples.filter(word -> "generally".equals(word._1())).first();
    Assert.assertEquals(2, generallyCouple._2().intValue());
  }

  @Test
  public void filterOnWordcount() {
    // run
    JavaPairRDD<String, Integer> filtered = ex0Wordcount.filterOnWordcount();

    // assert
    Assert.assertEquals(26, filtered.count());
    Tuple2<String, Integer> theCouple = filtered.filter(word -> "the".equals(word._1())).first();
    Assert.assertEquals(38, theCouple._2().intValue());
    JavaPairRDD<String, Integer> generallyCouple = filtered.filter(word -> "generally".equals(word._1()));
    Assert.assertEquals(0, generallyCouple.count());
  }
}
