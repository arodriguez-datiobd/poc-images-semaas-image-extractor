package com.datio.poc.usd

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging


class Configuration (file: String) extends LazyLogging {

  val config = ConfigFactory.parseResources(file)
  logger.info("Loaded configuration file: " + config.origin().filename())

  def getHdfsServer(): String = {
    config.getString("hdfs.server")
  }
}
