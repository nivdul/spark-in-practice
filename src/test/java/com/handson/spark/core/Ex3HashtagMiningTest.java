package com.handson.spark.core;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;

public class Ex3HashtagMiningTest {

  private Ex3HashtagMining ex3HashtagMining;

  @Before
  public void init() {
    ex3HashtagMining = new Ex3HashtagMining();
  }

  @Test
  public void mentionOnTweet() {
    // run
    JavaRDD<String> mentions = ex3HashtagMining.hashtagMentionedOnTweet();

    // assert
    Assert.assertEquals(5262, mentions.count());
    JavaRDD<String> filter = mentions.filter(mention -> "#youtube".equals(mention));
    Assert.assertEquals(2, filter.count());
  }

  @Test
  public void countMentions() {
    // run
    JavaPairRDD<String, Integer> counts = ex3HashtagMining.countMentions();

    // assert
    Assert.assertEquals(2461, counts.count());
    JavaPairRDD<String, Integer> filter = counts.filter(mention -> "#youtube".equals(mention._1()));
    Assert.assertEquals(1, filter.count());
    Assert.assertEquals(2, filter.first()._2().intValue());
  }

  @Test
  public void mostMentioned() {
    // run
    List<Tuple2<Integer, String>> mostMentioned = ex3HashtagMining.top10HashTags();

    // assert
    Assert.assertEquals(10, mostMentioned.size());
    Assert.assertEquals(253, mostMentioned.get(0)._1().intValue());
    Assert.assertEquals("#DME", mostMentioned.get(0)._2());
  }

}