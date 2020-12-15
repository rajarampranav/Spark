// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/238213666308599/1711259699546519/2707502659407016/latest.html

// COMMAND ----------

val data = sc.textFile("/FileStore/tables/numberData.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

val header = data.first

// COMMAND ----------

val datardd=data.filter(line => line!=header)

// COMMAND ----------

val datafinal=datardd.map(line=>line.toInt)

// COMMAND ----------

datafinal.take(3)

// COMMAND ----------

val primerdd = datafinal.filter(x => !((2 until x-1) exists ( x % _ ==0)) && x>1)

// COMMAND ----------

primerdd.collect

// COMMAND ----------

val reduceprime = primerdd.reduce( (x,y) => x+y)
reduceprime

// COMMAND ----------


