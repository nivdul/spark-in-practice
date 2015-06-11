package com.duchessfr.spark.utils;

import java.io.Serializable;

public class Tweet implements Serializable {

  long id;
  String user;
  String userName;
  String text;
  String place;
  String country;
  String lang;

  public String getUserName() {
    return userName;
  }

  public String getLang() {
    return lang;
  }

  public long getId() {
    return id;
  }

  public String getUser() { return user;}

  public String getText() {
    return text;
  }

  public String getPlace() {
    return place;
  }

  public String getCountry() {
    return country;
  }


  @Override
  public String toString(){
    return getId() + ", " + getUser() + ", " + getText() + ", " + getPlace() + ", " + getCountry();
  }
}