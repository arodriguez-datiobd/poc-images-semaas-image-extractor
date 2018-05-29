package com.datio.poc.usd

import java.io.{BufferedOutputStream, FileOutputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration


trait Writer extends LazyLogging {

  def write(filePath: String, fileName: String, data: Array[Byte])
}


/**
  * Writer for HDFS.
  *
  * It needs the configuration for HDFS
  *
  * @param conf Application configuration which includes the hdfs configuration with the server.
  */
class HdfsWriter(conf: com.datio.poc.usd.Configuration) extends Writer {

  def write(filePath: String, fileName: String, data: Array[Byte]) = {

    val path = new Path(filePath + fileName)
    val hdfsConf = new org.apache.hadoop.conf.Configuration()
    hdfsConf.set("fs.defaultFS", "hdfs://" + conf.getHdfsServer())
    val fs = FileSystem.get(hdfsConf)
    val os = fs.create(path)
    os.write(data)
    fs.close()

  }

}

/**
  * Writer for local file system.
  *
  */
class LocalWriter extends Writer {

  def write(filePath: String, fileName: String, data: Array[Byte]) = {

    val outputStream = new BufferedOutputStream(new FileOutputStream(filePath + fileName))
    outputStream.write(data)
    outputStream.close()
  }

}