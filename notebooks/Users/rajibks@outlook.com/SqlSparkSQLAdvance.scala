// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val defaultcustomerUrl = "wasbs://data@rajibksstorage.blob.core.windows.net/customer-orders.csv"
val schema = StructType(
Array(
StructField("userId",IntegerType,true),
StructField("OrderID",IntegerType,true),
StructField("OrderAmount",DoubleType,true)

)
)

val data = spark.read.option("inferSchema",false).option("header","false").option("sep",",").schema(schema).csv(defaultcustomerUrl)
data.schema
data.printSchema
data.createOrReplaceTempView("Orders")


// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE ProcessedOrders
// MAGIC   USING PARQUET
// MAGIC   PARTITIONED BY (userId)
// MAGIC   OPTIONS ('compression' = 'snappy')
// MAGIC   AS
// MAGIC   SELECT userId, SUM(orderAmount) AS totalAmount
// MAGIC     FROM orders
// MAGIC     GROUP BY userId
// MAGIC     ORDER BY totalAmount DESC
// MAGIC     LIMIT 10
// MAGIC   

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
val processedOrdersDF = spark.sql("SELECT userId, SUM(orderAmount) AS totalAmount FROM Orders GROUP BY userId ORDER BY totalAmount DESC LIMIT 10")
var bulkCopyMetadata = new BulkCopyMetadata
 


bulkCopyMetadata.addColumnMetadata(1, "userId", java.sql.Types.INTEGER, 0, 0)
bulkCopyMetadata.addColumnMetadata(2, "totalAmount", java.sql.Types.DOUBLE, 50, 0)


val bulkCopyConfig = Config(Map(
  "url"               -> "trainingrajibks.database.windows.net",
  "databaseName"      -> "Trainingsql",
  "user"              -> "rajibks",
  "password"          -> "Micr0soft@1",
  "dbTable"           -> "dbo.ProcessedOrders",
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "bulkCopyBatchSize" -> "2500",
  "bulkCopyTableLock" -> "true",
  "bulkCopyTimeout"   -> "600"
))

processedOrdersDF.bulkCopyToSqlDB(bulkCopyConfig, bulkCopyMetadata)