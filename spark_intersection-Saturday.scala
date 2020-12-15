// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/238213666308599/1905444717668464/2707502659407016/latest.html

// COMMAND ----------

val julyairport=sc.textFile("/FileStore/tables/nasa_19950701.tsv")
val augustairport=sc.textFile("/FileStore/tables/nasa_19950801.tsv")

// COMMAND ----------

val hostjuly = julyairport.map(x => x.split("\t")(0))
val hostaugust = augustairport.map(x => x.split("\t")(0))

// COMMAND ----------

val intersectionRDD = hostjuly.intersection(hostaugust)

// COMMAND ----------

val removeintersectionRDD = intersectionRDD.filter(line => !line.contains("host"))

// COMMAND ----------

removeintersectionRDD.collect()

// COMMAND ----------


