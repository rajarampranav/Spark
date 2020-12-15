// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/238213666308599/1615320530181638/2707502659407016/latest.html

// COMMAND ----------

val airport = sc.textFile("/FileStore/tables/airport_datas.text")

// COMMAND ----------

// DBTITLE 1,Latitude > 40 or country=Iceland
val filterairport = airport.filter(line => {(line.contains("\"Iceland\"")) || line.split(",")(6) > "\"40\""})

// COMMAND ----------

filterairport.saveAsTextFile("CountryIsland01.csv")

// COMMAND ----------

// DBTITLE 1,Count of even altitude and timestamp="Pacific/Port_Moresby"
val counts = airport.filter(line => {(line.split(",")(11).contains("Pacific/Port_Moresby")) && ((line.split(",")(8).toDouble)%2==0)})
counts.count()

// COMMAND ----------


