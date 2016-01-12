package com.founder.orgcube.util

import java.net.URI

import org.apache.http.HttpStatus
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils

import scala.collection.immutable.HashMap

/**
 * Created by lenovo on 2016/1/7.
 */
class HttpUtil {

  /**
   *以get方式访问http请求
   * @param path 访问的服务路径，http://xx.xx.xx.xx:8080/xxx
   * @param params 传递的参数，格式：参数名,参数值,参数名,参数值...
   */
 // def get(path:String,method:String,)
}

object HttpUtil{
  val connTimeout = 3000
  val socketTimeout = 3000
  val connRequestTimeout = 3000

  def get(path:String,httpmethod:String,params:Map[String,String]): String ={
    val httpClient =HttpClientBuilder.create().build()
    var httpResponse = null
    //var nvps:List[NameValuePair] = List()
    val uri = new URIBuilder(path)
    try{
      params.keys.foreach{key =>
        //nvps .::(new BasicNameValuePair(key, params(key)))
        uri.setParameters(new BasicNameValuePair(key, params(key)))
      }
      uri.build()
      val request = genRequest (uri.asInstanceOf[URI],httpmethod)
      val resp = httpClient.execute(request)
      if (resp.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        return EntityUtils.toString(resp.getEntity(), "utf-8")
      }else{
        println("http status code: "+ resp.getStatusLine().getStatusCode())
      }
    }

    "wls"
  }
  /**
   * 通过URI和请求方式生成httpclient HttpUriRequest
   * @param uri
   * @param httpmethod
   * @return
   */
  def  genRequest(uri:URI,httpmethod:String) :HttpUriRequest = {

    val request = matchRequest(uri,httpmethod)
    val config = RequestConfig.custom()
      .setConnectionRequestTimeout(connRequestTimeout)
      .setConnectTimeout(connTimeout)
      .setSocketTimeout(socketTimeout)
      .build()
    request.setConfig(config)
    request
  }

  //httpmethod =GET POST PUT
  def matchRequest(uri:URI,httpmethod:String):HttpRequestBase={
    httpmethod match {
      case "GET" => new HttpGet(uri)
      case "POST" =>new HttpPost(uri)
      case "PUT" => new HttpPut(uri)
      case _ => new HttpGet(uri)
    }
  }

  def main(args:Array[String]): Unit ={
    var map = new HashMap[String,String]
    map +=("method" -> "getUserInfo")
    map +=("userID" -> "1212")
    get("adfd","GET",map)
  }


}