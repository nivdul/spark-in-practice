package com.duchessfr.spark.streaming;

import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

/**
 *  Utilitary class to authenticate with the Twitter streaming API.
 *
 *  Go to https://apps.twitter.com/
 *  Create your application and then get your own credentials (keys and access tokens tab)
 */
public class StreamUtils {

  private static String CONSUMER_KEY = "PLEm9yaGqjHny80n9bHZUpsIf";
  private static String CONSUMER_SECRET = "UwlhNqo8P451yN5Hh6VBlnSiJqRv032QSte9VpOQqH3kAtneXM";
  private static String ACCESS_TOKEN = "493498548-UTAPXQ7D5FFfGZSMSMibzKEe9ddI0ImSP5UfW1dS";
  private static String ACCESS_TOKEN_SECRET = "T0QVt5crqYmeOH5nVl4P7KFx52uCUMRqhKEF2ZUHLFMJO";

  public static OAuthAuthorization getAuth() {

    return new OAuthAuthorization(
        new ConfigurationBuilder().setOAuthConsumerKey(CONSUMER_KEY)
            .setOAuthConsumerSecret(CONSUMER_SECRET)
            .setOAuthAccessToken(ACCESS_TOKEN)
            .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
            .build());
  }
}
