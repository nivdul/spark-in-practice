package com.handson.spark.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class Parse {

  public static Tweet parseJsonToTweet(String jsonLine) {

    ObjectMapper objectMapper = new ObjectMapper();
    Tweet tweet = null;

    try {
      tweet = objectMapper.readValue(jsonLine, Tweet.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return tweet;
  }

}
