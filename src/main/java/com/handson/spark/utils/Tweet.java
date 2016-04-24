package com.handson.spark.utils;

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

  public void setId(long id) {
    this.id = id;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public void setText(String text) {
    this.text = text;
  }

  public void setPlace(String place) {
    this.place = place;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public void setLang(String lang) {
    this.lang = lang;
  }

  @Override
  public String toString(){
    return getId() + ", " + getUser() + ", " + getText() + ", " + getPlace() + ", " + getCountry();
  }
}