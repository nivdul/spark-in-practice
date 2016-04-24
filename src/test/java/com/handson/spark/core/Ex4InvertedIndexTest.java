package com.handson.spark.core;


import com.handson.spark.utils.Tweet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

public class Ex4InvertedIndexTest {

  private Ex4InvertedIndex ex4InvertedIndex;

  @Before
  public void init() {
    ex4InvertedIndex = new Ex4InvertedIndex();
  }

  @Test
  public void invertedIndex() {
    // run
    Map<String, Iterable<Tweet>> map = ex4InvertedIndex.invertedIndex();

    // assert
    Assert.assertEquals(2461, map.size());
    Iterable<Tweet> paris = map.get("#Paris");
    int count = 0;
    Iterator<Tweet> iter = paris.iterator();
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    Assert.assertEquals(144, count);
    Iterable<Tweet> edc = map.get("#EDC");
    int count2 = 0;
    Iterator<Tweet> iter2 = edc.iterator();
    while (iter2.hasNext()) {
      iter2.next();
      count2++;
    }
    Assert.assertEquals(1, count2);
  }
}