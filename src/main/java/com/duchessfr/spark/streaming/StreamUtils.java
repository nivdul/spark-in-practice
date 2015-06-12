package com.duchessfr.spark.streaming;

import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

/**
 *  Utilitary class to authenticate with the Twitter streaming API.
 *
 *  Go to https://apps.twitter.com/
 *  Create your application and then get your own credentials (keys and access tokens tab)
 *
 *  See https://databricks-training.s3.amazonaws.com/realtime-processing-with-spark-streaming.html
 *  for help.
 *
 *  If you have the following error "error 401 Unauthorized":
 *  - it might be because of wrong credentials
 *  OR
 *  - a time zone issue (so be certain that the time zone on your computer is the good one)
 *
 */
public class StreamUtils {

  private static String CONSUMER_KEY = "AFiNChb80vxYZfhPls2DXyDpF";
  private static String CONSUMER_SECRET = "JRg7SWyVFkXEESWbEzFzC1xaIGRC3xNdTvrekMvMFk6tjKooOR";
  private static String ACCESS_TOKEN = "493498548-HCCt6LCposCb3Ij7Ygt7ssTxTBPwGoPrnkkDQoaN";
  private static String ACCESS_TOKEN_SECRET = "3pxA3rnBzWa9bmOmOQPWNMpYc4qdOrOdxGFgp6XiCkEKH";

  public static OAuthAuthorization getAuth() {

    return new OAuthAuthorization(
        new ConfigurationBuilder().setOAuthConsumerKey(CONSUMER_KEY)
            .setOAuthConsumerSecret(CONSUMER_SECRET)
            .setOAuthAccessToken(ACCESS_TOKEN)
            .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
            .build());
  }
}
