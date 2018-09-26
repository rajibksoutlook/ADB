// Databricks notebook source
// MAGIC %md
// MAGIC #Learning ADB Professionally

// COMMAND ----------

val defaultMoviesUrl = "https://rajibksstorage.blob.core.windows.net/data1/movies.csv"
val defaultRatingsUrl = "adl://rajibdatalakestore.azuredatalakestore.net/data/ratings.csv"

val moviesUrl = dbutils.widgets.text("moviesUrl","")
val ratingsUrl = dbutils.widgets.text("ratingsUrl", "")

var inputMoviesUrl = dbutils.widgets.get("moviesUrl")

if(inputMoviesUrl == null) {
  inputMoviesUrl = defaultMoviesUrl
}

var inputRatingsUrl = dbutils.widgets.get("ratingsUrl")

if(inputRatingsUrl == null) {
  inputRatingsUrl = defaultRatingsUrl
}

// COMMAND ----------

package com.Microsoft.analytics.utils
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
object MovieUtils1 {
def loadMovieNames1(fileName: String): Map[Int, String] = {
  if(fileName == null || fileName == "") {
    throw new Exception("Invalid File / Reference URL Specified!");
  }

  implicit val codec = Codec("UTF-8")

  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  val lines = Source.fromURL(fileName).getLines

  lines.drop(1)

  var movieNames: Map[Int, String] = Map()

  for(line <- lines) {
    val records = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
    val movieId = records(0).toInt
    val movieName = records(1)

    movieNames += (movieId -> movieName)
  }

  movieNames
}
}


// COMMAND ----------

import com.Microsoft.analytics.utils._
var broadcastmoview=sc.broadcast(()=>{MovieUtils1.loadMovieNames1(inputMoviesUrl)})

// COMMAND ----------

spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "7ef70fe7-b9c3-4394-89c1-dbb38290a4e7")
spark.conf.set("dfs.adls.oauth2.credential", "aapIjVpm+Mhxn64QOksMV3Kb7q2CeJ1wu+I56FgGeQI=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/bcd98700-945b-4734-8829-52c28b9c13c9/oauth2/token")

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type", spark.conf.get("dfs.adls.oauth2.access.token.provider.type"))
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id", spark.conf.get("dfs.adls.oauth2.client.id"))

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential", spark.conf.get("dfs.adls.oauth2.credential"))

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url", spark.conf.get("dfs.adls.oauth2.refresh.url"))

val ratingsData = sc.textFile("adl://rajibdatalakestore.azuredatalakestore.net/data/ratings.csv")
val originalData = ratingsData.mapPartitionsWithIndex((index, iterator) => {
if(index == 0) iterator.drop(1)

 else iterator
})
val mappedData = originalData.map(line => { val splitted = line.split(",")

(splitted(1).toInt, 1)
})
val reducedData = mappedData.reduceByKey((x, y) => (x + y))
val result = reducedData.sortBy(_._2).collect
val finalOutput = result.reverse.take(10)
val mappedFinalOuptut = finalOutput.map(record => (broadcastmoview.value()(record._1), record._2))

// COMMAND ----------

mappedFinalOuptut.foreach(println)

// COMMAND ----------

