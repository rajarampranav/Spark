// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/238213666308599/4364251242089860/2707502659407016/latest.html

// COMMAND ----------

val friends = sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

val removefriends = friends.filter(line => !line.contains("Id"))

// COMMAND ----------

val friendrdd = removefriends.map(x => (x.split(",")(2).toInt,(1,x.split(",")(3).toInt)))

// COMMAND ----------

val avgfriend = friendrdd.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))

// COMMAND ----------

// DBTITLE 1,Average for friends age
val reduced = avgfriend.mapValues(data => data._2/data._1)
reduced.collect()

// COMMAND ----------

// DBTITLE 1,Maximum friends per age
val maxage = friendrdd.reduceByKey((x,y) => (x._1,math.max(y._1,y._2)))

// COMMAND ----------

maxage.collect()

// COMMAND ----------



// COMMAND ----------


