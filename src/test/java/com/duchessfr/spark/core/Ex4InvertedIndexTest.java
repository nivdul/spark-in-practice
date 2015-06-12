package com.duchessfr.spark.core;


import com.duchessfr.spark.utils.Tweet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    Assert.assertEquals(2113, map.size());
  }
}