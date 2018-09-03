package net.lubet.spaif

import java.net.URL
import java.io.IOException

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import org.jsoup.Connection.Method

import collection.JavaConversions._

object Browser {
  def get(url: URL): String = {
    Jsoup.connect(url.toString)
      .userAgent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0")
      .referrer("https://www.euronext.com/")
      .header("Host", "www.euronext.com")
      .header("Upgrade-Insecure-Requests", "1")
      .header("Connection", "keep-alive")
      .header("DNT", "1")
      .header("Accept-Encoding", "gzip, deflate, br")
      .header("Accept-Language", "en-US,en;q=0.5")
      .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8").
      //.ignoreHttpErrors(true)
      //.cookie("datadome", "AHrlqAAAAAMADf4SVA8s7HIAIvVOkQ==")
    ignoreContentType(true).
      followRedirects(true).execute.body
  }

  def post(url: URL, data: Map[String, String]): String = {
    Jsoup.connect(url.toString).
      userAgent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0").
      referrer("https://www.euronext.com/").
      header("Host", "www.euronext.com").
      header("Upgrade-Insecure-Requests", "1").
      header("Connection", "keep-alive").
      header("DNT", "1").
      header("Accept-Encoding", "gzip, deflate, br").
      header("Accept-Language", "en-US,en;q=0.5").
      header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8").
      timeout(10 * 1000).
      method(Method.POST).
      data(mapAsJavaMap(data)).
      followRedirects(true).execute.body
  }
}