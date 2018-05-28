package com.datio.poc.usd

import scalaj.http.{Http, HttpResponse}
import java.io.{BufferedOutputStream, FileOutputStream}
import io.gatling.jsonpath.JsonPath
import com.fasterxml.jackson.databind.ObjectMapper



//import com.typesafe.scalalogging.LazyLogging

//class SemaasRestClient extends LazyLogging {


object SemaasRestClient {

  val ENDPOINT_URL = "https://epsilon.play.global.semaas-spot.com/v2/ns/arodriguez.test/buckets/test.images.bucket/files"
  val INITIAL_RESOURCE = "ns/arodriguez.test/buckets/test.images.bucket/files"
  val API_KEY = "94db014b-d2ab-4bee-84a8-79f010ea4ac6"
  val SERVICE_URL = "epsilon.play.global.semaas-spot.com"
  val PROTOCOL = "https"
  val VERSION = "v2"
}


class SemaasRestClient(saveFolder: String) {

  import SemaasRestClient.{API_KEY, ENDPOINT_URL, INITIAL_RESOURCE, SERVICE_URL, PROTOCOL, VERSION}

  //  logger.debug("This is very convenient ;-)")



  val response = getFiles(PROTOCOL + "://" + SERVICE_URL + "/" + VERSION + "/" + INITIAL_RESOURCE)
  parseResponse(response) match {
    case Some(filesURL : List[String]) => (filesURL.map(downloadFile(_))).foreach(binaryFile => saveFile(binaryFile.get._2, saveFolder, binaryFile.get._1))

    case None => println("Empty bucket, no files to download")
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
    * @return List of _locator
    */
  def parseResponse(response : String): Option[List[(String)]] = {

    //TODO change this method to return a tuple with the filename and the url endpoint. Include here the parsing and pattern matching code.
    val jsonSample = (new ObjectMapper).readValue(response, classOf[Object])

    JsonPath.query("$.files[*]._locator", jsonSample) match {
      case Left(errorMsg) =>
        println(errorMsg)
        None
      case Right(iterator) => Some(iterator.toList.map(_.toString))

    }

  }


  /**
    * Retrieves the binary resource from SEMaaS bucket
    *
    * @param resource The resource to download, it is a binary file stored on a bucket
    * @return A tuple which contains, the file name of the resource, the file (binary data)
    *
    */
  def downloadFile(resource: String): Option[(String, Array[Byte])] = {

    // TODO move all the pattern matching to parseResponse method. This method should be structure agnostic.

    val nsPattern = "(.*)(/ns/.*)".r
    val nsPattern(server, rs) = resource

    val endpoint: String = PROTOCOL + "://" + SERVICE_URL + "/" + VERSION + rs + ":download"
    println("Downloading: " + endpoint)

    val filePattern = "(.*?)([^/]*\\.\\w{3}$)".r
    val filePattern(path, filename) = resource
    println(filename)

    try {
      val response: HttpResponse[Array[Byte]] = Http(endpoint)
        .header("API-KEY", API_KEY)
        .timeout(connTimeoutMs = 2000, readTimeoutMs = 5000)
        .asBytes

      Some(filename, response.body)
    } catch  {
      case t: Throwable =>
        println("Error downloading file from: " + endpoint)
        println(t)
        None
    }
  }


  /**
    * Stores the binary data in the path with the specified filename
    *
    * @param byteArray File content in binary format
    * @param path Path where file should be stored
    * @param fileName The file name to store the file
    *
    */
  def saveFile(byteArray: Array[Byte], path: String, fileName: String): Unit = {

    println("File path for saving: " + path + fileName)
    val outputStream = new BufferedOutputStream(new FileOutputStream(path + fileName))
    outputStream.write(byteArray)
    outputStream.close()

  }

}
