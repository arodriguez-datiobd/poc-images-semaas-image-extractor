package com.datio.poc.usd


/**
  * Hello world!
  *
  */
object Main extends App {

  val config = new Configuration("application.conf")

  val rc = new SemaasRestClient("/tmp/", new HdfsWriter(config))

}
