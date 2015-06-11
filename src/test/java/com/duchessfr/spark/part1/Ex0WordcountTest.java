package com.duchessfr.spark.part1;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


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
  }

  @Test
  public void filterOnWordcount() {
    // run
    JavaPairRDD<String, Integer> filtered = ex0Wordcount.filterOnWordcount();

    // assert
    Assert.assertEquals(26, filtered.count());
  }
}
