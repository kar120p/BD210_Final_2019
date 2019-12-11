// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## UW BD210 Data Engineering Final
// MAGIC -------
// MAGIC #### Wholesale Electricity LBMP & Irradiance Adjusted PV Revenue
// MAGIC -------
// MAGIC I began the project downloading hourly locational based marginal price (LBMP) data from the PJM website, which is the price seen at power plants, and other interfaces with the grid across their service territory in the Mid/South-Atlantic and Midwest.
// MAGIC 
// MAGIC The price is determined in a day-ahead auction by PJM, the market operator, and their models designed to reflect in the price the weights of weather, constraints on transmission lines, other environmental variables, and all bids into the auction from regional generators/loads for their cost to produce a MW at that hour. In 2018, for reduced dataset I chose there were 4688 unique pnodes (pricing nodes) with complete annual data, or 41,066,880 entries and a ~3GB csv file.
// MAGIC 
// MAGIC 
// MAGIC **Links**
// MAGIC 
// MAGIC [PJM's LMP Model Info](https://www.pjm.com/markets-and-operations/energy/lmp-model-info.aspx) | [PJM Tools Data Access](https://www.pjm.com/markets-and-operations/etools.aspx)
// MAGIC 
// MAGIC ____
// MAGIC 
// MAGIC 
// MAGIC #### Further data considered
// MAGIC 
// MAGIC ___Joining by location and date to determine where and when solar facilities might best capture revenue from unique market areas and their price swings___ 
// MAGIC 
// MAGIC -----
// MAGIC 
// MAGIC **Solar irradiance data from NREL**
// MAGIC 
// MAGIC [NREL NSRDB Data Viewer](https://maps.nrel.gov/nsrdb-viewer/)
// MAGIC 
// MAGIC **Reference case of hourly modeled production at a solar pv facility for one year**
// MAGIC 
// MAGIC N/A
// MAGIC 
// MAGIC ----
// MAGIC 
// MAGIC #### Goals
// MAGIC 
// MAGIC ___
// MAGIC 
// MAGIC * Determine annual price averages at pricing nodes
// MAGIC * Determine annual revenues captured by a solar facility
// MAGIC * Determine highest value nodes and regions for solar
// MAGIC * Present findings in Map
// MAGIC * Complete analysis for multiple years (incomplete)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Imports, Load Data, View

// COMMAND ----------

//import libraries, route to dbfs
val root = "dbfs:/autumn_2019/pkampf/bd210_final/pjm_test/"
import sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

// COMMAND ----------

//mount azure blob storage directories
dbutils.fs.ls("abfss://pkampf@uwbigdatatechnologies.dfs.core.windows.net/pjm_da_lmp")
dbutils.fs.ls("abfss://pkampf@uwbigdatatechnologies.dfs.core.windows.net/pjm_joins")

// COMMAND ----------

//read in pricing data csv(s) from azure blob storage
val pjm = (spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("abfss://pkampf@uwbigdatatechnologies.dfs.core.windows.net/pjm_da_lmp/pjm_da_lmp_merge_2018.csv"))

// COMMAND ----------

// write larger file to parquet file in dbfs
pjm.write.parquet(root + "pjm2018.parquet")

// COMMAND ----------

//read back in from parquet
val pjm_df = sqlContext.read.parquet(root + "pjm2018.parquet")

// COMMAND ----------

pjm_df.show(5)

// COMMAND ----------

//
val nerc = (spark.read
  .option("header", "true")
  .csv("abfss://pkampf@uwbigdatatechnologies.dfs.core.windows.net/pjm_joins/pjm_nerc_holidays_0021.csv"))

//read in 
val pvprod = (spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("abfss://pkampf@uwbigdatatechnologies.dfs.core.windows.net/pjm_joins/pa_8760_map.csv"))

// read in irradiance data
val ghi = (spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("abfss://pkampf@uwbigdatatechnologies.dfs.core.windows.net/pjm_joins/pjm_zipcode_nodes_ghi.csv"))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Cleaning Data

// COMMAND ----------

ghi.orderBy($"zipcode".asc).show(5)

// COMMAND ----------

val ghi_map = ghi.drop("ZCTA5CE10", "zipcode_co" )

// COMMAND ----------

//convert column to timestamp
val ts = to_timestamp($"hol_date", "yyyy-MM-dd")
val nerc_date = nerc.withColumn("datetime", ts)
//create formatted datetime column
val nercd = nerc_date.withColumn("date", date_format(col("datetime"), "yyyy-MM-dd"))

// COMMAND ----------

//order pricing df by datetime, check last row
pjm_df.orderBy($"datetime_beginning_ept".desc).show(1) 

// COMMAND ----------

//drop unneeded columns
val pjm_drop = pjm_df.drop("datetime_beginning_utc")
//define new column order
val newCol = Seq("timestamp", "pnode", "name", "vtg", "zone", "lbmp")
val pjm1 = pjm_drop.toDF(newCol:_*)

// COMMAND ----------

//create new timestamp column by defining function object, and passing to withColumn
val ts = to_timestamp($"timestamp", "MM/dd/yyyy hh:mm:ss a")
val pjm_tz = pjm1.withColumn("datetime", ts)
//drop unformatted/timezone unaware timestamp column
val pjm_ts = pjm_tz.drop("timestamp")

// COMMAND ----------

//drop unformatted/timezone unaware timestamp column
val pjm_ts = pjm_tz.drop("timestamp")
//reorder and rename columns
val columns = pjm_ts.columns
val reorderedColumnNames = Seq("datetime", "pnode", "name", "vtg", "zone", "lbmp")
val pjm_ = pjm_ts.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
//rename again
val pjm_ord = pjm_.withColumnRenamed("datetime","ts")

// COMMAND ----------

pjm_ord.orderBy($"ts".desc).show(1) 

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Further manipulations, working on Pricing Data sheet to gather features from timestamp

// COMMAND ----------

//udfs to convert schemas
val toInt = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)
val intString = udf[String, Int](_.toString)

// COMMAND ----------

//create datetime attributes from timestamp column
// weekday "u" range 1 monday-7 sunday
val pjm_tz = pjm_ord.withColumn("date", date_format(col("ts"), "yyyy-MM-dd"))
.withColumn("year", year(col("ts")))
.withColumn("month", month(col("ts")))
.withColumn("monthday", toInt(date_format(col("ts"), "d")))
.withColumn("weekday", toInt(date_format(col("ts"), "u")))
.withColumn("dayofyear", toInt(date_format(col("ts"), "D")))
.withColumn("hour", hour(col("ts")))

// COMMAND ----------

//sanity check count of distinct values per column
pjm_tz.select(pjm_tz.columns.map(c => countDistinct(col(c)).alias(c)): _*).show()

// COMMAND ----------

//check rowcount to ensure we're not dropping data
pjm_tz.count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Appending Data from holiday dates to determine Peak and off-Peak Times

// COMMAND ----------

//create list of holidays to check in 
val holiday = nercd.select("date").map(r => r.getString(0)).collect.toList

// COMMAND ----------

//create new df with column indicating date a holiday or not t/f
val pjm_holi = pjm_tz.withColumn("holiday", col("date").isin(holiday: _*))

// COMMAND ----------

//another sanity check to make sure values appear in holiday column and we didn't get extra rows
pjm_holi.select(pjm_holi.columns.map(c => countDistinct(col(c)).alias(c)): _*).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Determine with when if peak hour or not

// COMMAND ----------

val pjm_pk = pjm_holi.withColumn("is_peak", lit("on"))
val peak_df = pjm_pk.withColumn("is_peak",
                                  when($"hour" >= 23, "off")
                                  .when($"hour" < 7 , "off")
                                  .when($"weekday" >= 6, "off")
                                  .when($"holiday" === 1, "off")
                                  .otherwise("on"))

// COMMAND ----------

//counting peak/off-peak hours and total row count
println("Count off peak: "  + peak_df.filter($"is_peak".contains("off")).count())
println("Count on peak: " + peak_df.filter($"is_peak".contains("on")).count())
println("Count rows: " + peak_df.count())

// COMMAND ----------

// MAGIC %md
// MAGIC #### Create month.day.hour tag to join 8760 and production data

// COMMAND ----------

val pjm_peak = peak_df.withColumn("month_s", intString($"month"))
.withColumn("monthday_s", intString($"monthday"))
.withColumn("hour_s", intString($"hour"))
.withColumn("8760_tag", concat_ws(".", $"month_s", $"monthday_s", $"hour_s"  ))
.drop("month_s", "monthday_s", "hour_s" )

// COMMAND ----------

pjm_peak.show(5)

// COMMAND ----------

//assign new column names to production data to avoid dropping conflicts
val newCol = Seq("timestamp", "8760_hour", "net_energy_kW", "net_energy_MW", "month_", "day_", "8760_date", "prodhour")
val pvprodu = pvprod.toDF(newCol:_*)

// COMMAND ----------

//left join production data on date tag
val pjm_pv = pjm_peak.join(pvprodu, pjm_peak("8760_tag") === pvprodu("8760_date"), "left").drop("timestamp", "net_energy_kW", "month_", "day_", "prodhour")

// COMMAND ----------

pjm_pv.show(2)

// COMMAND ----------

//sanity check confirm sums at each node the same
pjm_pv.groupBy("pnode").agg(sum("net_energy_MW")).show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Joining Mean GHI to nodes to discount revenues based on Irradiance

// COMMAND ----------

//adjust ghi to production model source data as center
val ghi_adj = ghi_map.withColumn("ghi_adj_mean", $"ghi_mean"/4.104)

// COMMAND ----------

//rename ghi/location data to avoid naming conflicts
val newCol = Seq("zipcode", "city", "state", "pnode_id", "pnode_name", "ghi_min", "ghi_max", "ghi_mean", "ghi_stddev", "ghi_adj_mean")
val ghi_adju = ghi_adj.toDF(newCol:_*)

// COMMAND ----------

ghi_adju.show(2)

// COMMAND ----------

//join ghi and location data onto larger df, join on node ID
val pjm_ghi = pjm_pv.join(ghi_adju, pjm_pv("pnode") === ghi_adju("pnode_id"), "left").drop("ghi_min", "ghi_max", "ghi_stddev", "8760_tag", "pnode_id", "pnode_name")

// COMMAND ----------

display(pjm_ghi)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Calculating revenues and discount totals based on ghi associated at region

// COMMAND ----------

val pjm_rev = pjm_ghi.withColumn("revenue", $"lbmp"* $"net_energy_MW")

// COMMAND ----------

// MAGIC %md
// MAGIC ___Aggregating and pivoting, then discounting annual sum Revenue___

// COMMAND ----------

//methods for pivoting/aggregating fields
val pjm_pivot = pjm_rev.groupBy("pnode").pivot("year").agg(avg("lbmp"), count("lbmp"), sum("revenue").alias("annual_revenue"), max("ghi_adj_mean"))
//adjust revenues
val pjm_pivot_adj = pjm_pivot.withColumn("adj_revenue", $"2018_annual_revenue"*$"2018_max(ghi_adj_mean)")

// COMMAND ----------

//final df to inspect, filtering for incomplete years
val pjm_reduced = pjm_pivot_adj.filter($"2018_count(lbmp)" === 8760).sort($"adj_revenue".desc)

// COMMAND ----------

pjm_reduced.show(10)

// COMMAND ----------

display(pjm_pivot_adj.filter($"2018_count(lbmp)" === 8760).sort($"adj_revenue".desc))

// COMMAND ----------


