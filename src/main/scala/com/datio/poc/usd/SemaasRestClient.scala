package com.datio.poc.usd

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging
import io.gatling.jsonpath.JsonPath
import scalaj.http.{Http, HttpResponse}


object SemaasRestClient {

  val ENDPOINT_URL = "https://epsilon.play.global.semaas-spot.com/v2/ns/arodriguez.test/buckets/test.images.bucket/files"
  val INITIAL_RESOURCE = "ns/arodriguez.test/buckets/test.images.bucket/files"
  val API_KEY = "94db014b-d2ab-4bee-84a8-79f010ea4ac6"
  val SERVICE_URL = "epsilon.play.global.semaas-spot.com"
  val PROTOCOL = "https"
  val VERSION = "v2"
}

class SemaasRestClient(saveFolder: String, writer: Writer) extends LazyLogging {

  import SemaasRestClient._


  val response = getFiles(PROTOCOL + "://" + SERVICE_URL + "/" + VERSION + "/" + INITIAL_RESOURCE)
  parseResponse(response) match {
    //    case Some(files : List[(String, String)]) => (files.map(x => ((downloadFile(_1), _2))).foreach(binaryFile => saveFile(binaryFile._2, saveFolder, binaryFile._1)))
    case Some(filesURL: List[(String, String)]) => (filesURL.map(x => (x._1, downloadFile(x._2)))).foreach(entry => saveFile(entry._2.get, saveFolder, entry._1))
    case None => logger.info("Empty bucket, no files to download")
  }


  /**
    * Retrieves the list of files in the bucket. It returns the following structure:
    *
    * {
    * "files": [
    * {
    * "_owner": "alejandra.paniagua.contractor@bbva.com",
    * "_ac": false,
    * "_id": "contrato_3_0_4557.jpg",
    * "_type": "epsilon.file",
    * "_parent": "//epsilon.play.global/ns/arodriguez.test/buckets/test.images.bucket/files",
    * "_locator": "//epsilon.play.global/ns/arodriguez.test/buckets/test.images.bucket/files/contrato_3_0_4557.jpg",
    * "createdAt": 1527151742373000000,
    * "updatedAt": 1527151742373000000,
    * "length": 7127
    * },
    * {
    * "_owner": "alejandra.paniagua.contractor@bbva.com",
    * "_ac": false,
    * "_id": "contrato_3_0_7060.jpg",
    * "_type": "epsilon.file",
    * "_parent": "//epsilon.play.global/ns/arodriguez.test/buckets/test.images.bucket/files",
    * "_locator": "//epsilon.play.global/ns/arodriguez.test/buckets/test.images.bucket/files/contrato_3_0_7060.jpg",
    * "createdAt": 1527151761238000000,
    * "updatedAt": 1527151761238000000,
    * "length": 7379
    * }
    * ],
    * "nextPageToken": ""
    * }
    *
    * @param url The URL of the bucket
    * @return A list of files following the previous schema
    */
  def getFiles(url: String): String = {

    logger.debug("Retrieving data from: " + url)

    val response: HttpResponse[String] = Http(url)
      .header("API-KEY", API_KEY)
      .timeout(connTimeoutMs = 2000, readTimeoutMs = 5000)
      .asString

    response.body
  }

  /**
    * It parses the response retrieved from getFiles method to generate a list with just the files to download.
    *
    * The _locator attribute does not specify the endpoint for traversing the resource structure, it represents a logical resource location.
    *
    * @param response the response structure defined in getFiles.
    * @return List with a tuple (file name, file resource url for downloading)
    */
  def parseResponse(response: String): Option[List[(String, String)]] = {

    val jsonSample = (new ObjectMapper).readValue(response, classOf[Object])

    JsonPath.query("$.files[*]._locator", jsonSample) match {
      case Left(errorMsg) =>
        // TODO verify if reason is the correct field
        logger.error("Error parsing data. " + errorMsg.reason)
        None
      case Right(iterator) => Some(iterator.toList.map(x => (parseFileName(x.toString), getResourceDownloadUrl(x.toString))))

    }

  }

  /**
    * Given a _locator string this method retrieves the resource file name
    *
    * @param resource The _locator string retrieved from semaas
    * @return The file name used in the _locator
    *
    */
  def parseFileName(resource: String): String = {

    val filePattern = "(.*?)([^/]*\\.\\w{3}$)".r
    val filePattern(path, filename) = resource
    filename

  }

  /**
    * Given a _locator string with the resource the method generates the URL for downloading the resource
    *
    * @param resource The _locator string retrieved from semaas
    * @return The resource URL for downloading to the resource
    */
  def getResourceDownloadUrl(resource: String): String = {

    val nsPattern = "(.*)(/ns/.*)".r
    val nsPattern(server, rs) = resource

    val endpoint: String = PROTOCOL + "://" + SERVICE_URL + "/" + VERSION + rs + ":download"
    endpoint

  }


  /**
    * Retrieves the binary resource from SEMaaS bucket
    *
    * @param endpoint The resource to download, it is a binary file stored on a bucket
    * @return A tuple which contains, the file name of the resource, the file (binary data)
    *
    */
  def downloadFile(endpoint: String): Option[(Array[Byte])] = {

    logger.debug("Downloading: " + endpoint)

    try {
      val response: HttpResponse[Array[Byte]] = Http(endpoint)
        .header("API-KEY", API_KEY)
        .timeout(connTimeoutMs = 2000, readTimeoutMs = 5000)
        .asBytes

      Some(response.body)
    } catch {
      case t: Throwable =>
        logger.error("Error downloading file from: " + endpoint)
        logger.debug(t.getMessage)
        None
    }
  }


  /**
    * Stores the binary data in the path with the specified filename
    *
    * @param byteArray File content in binary format
    * @param path      Path where file should be stored
    * @param fileName  The file name to store the file
    *
    */
  def saveFile(byteArray: Array[Byte], path: String, fileName: String): Unit = {
    writer.write(path, fileName, byteArray)

  }


}
