package com.duchessfr.spark.dataframe;


import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataFrameOnTweetsTest {

  private DataFrameOnTweets dataFrame;

  @Before
  public void init() {
    dataFrame = new DataFrameOnTweets();
  }


  @Test
  public void showDataFrame() {
    // run
    dataFrame.showDataFrame();

    // assert
    // you must see something like that in your console:
    //+--------------------+------------------+-----------------+--------------------+-------------------+
    //|             country|                id|            place|                text|               user|
    //+--------------------+------------------+-----------------+--------------------+-------------------+
    //|               India|572692378957430785|           Orissa|@always_nidhi @Yo...|    Srkian_nishu :)|
    //|       United States|572575240615796737|        Manhattan|@OnlyDancers Bell...| TagineDiningGlobal|
    //|       United States|572575243883036672|        Claremont|1/ "Without the a...|        Daniel Beer|
  }

  @Test
  public void printSchema() {
    // run
    dataFrame.printSchema();

    // assert
    // you must see something like that in your console:
    // root
    //    |-- country: string (nullable = true)
    //    |-- id: string (nullable = true)
    //    |-- place: string (nullable = true)
    //    |-- text:string(nullable = true)
    //    |-- user: string (nullable = true)
  }

  @Test
  public void filterByLocation() {
    // run
    DataFrame result = dataFrame.filterByLocation();

    // assert
    Assert.assertEquals(329, result.count());

  }

  @Test
  public void mostPopularTwitterer() {
    // run
    Row result = dataFrame.mostPopularTwitterer();

    // assert
    Assert.assertEquals("#QuissyUpSoon", result.get(0));
    Assert.assertEquals(258L, result.get(1));

  }

}
