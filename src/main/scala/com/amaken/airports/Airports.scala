package com.amaken.airports

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


class Airports {

  val spark: SparkSession = SparkSession.builder().getOrCreate()

  implicit val sc: SparkContext = spark.sparkContext
  implicit val sQLContext: SQLContext = spark.sqlContext
  //import spark.sqlContext.implicits._


  def loadCSV(path: String, options: Map[String,String] = Map.empty[String,String])(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext
      .read
      .format(options.getOrElse("format", "csv"))
      .option("header", options.getOrElse("header", "true"))
      .option("inferSchema", options.getOrElse("inferSchema", "true"))
      .load(path)

  }


  val parseUDF: UserDefinedFunction = udf((code: String, name: String, city: String, country: String, major: Boolean) => {
    val name2 = if (major) s"$name (All Airports)" else name
    val regex = """National|Harbour|Memorial|Rgnl|River|Port|Lake|Bay|City|Island|County|Field|Regional|International|Municipal|Intl|Airport|Arpt|arpt|ARPT|\s+(Intl)\s+""".r

    def clean(input: String) = regex.replaceAllIn(input, " ").trim.replaceAll("( )+", " ").split("( )").toSet.toList.mkString("\"", "\",\"", "\"")

    if (major)
      s"""{"name": "$name2","code": "$code","country": "$country","value": "$code","tokens": ["$city","$code","$country"]}"""
    else
      s"""{"name": "$name2","code": "$code","country": "$country","cityname": "$name","value": "$code","tokens": ["$city","$code","$country",${clean(name)}]}"""
  })

  val nameWithSynUDF: UserDefinedFunction = udf((name: String, sName: String) => { if (Option(sName).exists(_.nonEmpty)) s"""$name ($sName)""" else s"""$name""" })

  val nameWithSynSpaceUDF: UserDefinedFunction = udf((name: String, sName: String) => { if (Option(sName).exists(_.nonEmpty)) s"""$name $sName""" else s"""$name""" })

  val arrayToStringUDF: UserDefinedFunction = udf((input: Array[String]) => { input.toList.mkString(",") })

  val majorCitiesDF: DataFrame = loadCSV("data/input/major_cities.csv")  //spark.read.table("major_cities").persist

  majorCitiesDF.count
  majorCitiesDF.show(2, truncate = false)

  val newMajorCitiesDFOut: DataFrame = majorCitiesDF.withColumn("Output", parseUDF(col("Code"),col("Name"),col("City"),col("Country"), lit(true))).select(col("Output"))

  newMajorCitiesDFOut.show(10, truncate = false)


  val citiesDF: DataFrame =  loadCSV("data/input/cities.csv") //spark.read.table("cities").persist

  citiesDF.count
  citiesDF.show(2, truncate = false)

  val citiesSynonymsDF: DataFrame = citiesDF.filter(col("Synonym") === "S").select(col("CityCode"),col("Name").as("citySynonymousName"))

  citiesSynonymsDF.count

  val citiesWithAllSynsDF: DataFrame = citiesSynonymsDF.groupBy(col("CityCode")).agg(collect_set(col("citySynonymousName")))
    .select(col("CityCode"), concat_ws(" ", collect_set(col("citySynonymousName"))).as("Names"))

  citiesWithAllSynsDF.printSchema
  citiesWithAllSynsDF.show(15, truncate = false)

  val airportsDF: DataFrame = loadCSV("data/input/airports.csv") //spark.read.table("airports").persist
  airportsDF.count

  val airportsFinalDF: Dataset[Row] = airportsDF.filter( col("Type") === 1 || col("Type") === 2 || col("Type") === 3 ).persist
  airportsFinalDF.count

  val airDF: DataFrame = airportsFinalDF.select(col("Code"),col("Name"),col("City"),col("Country"),col("Synonym")).persist

  airDF.count
  airDF.show(15, truncate = false)
  airDF.printSchema
  citiesDF.filter(col("CityCode")==="MIL").show(5, truncate = false)
  citiesDF.printSchema

  val citiesFinalDF: DataFrame = citiesDF
    .filter(col("Synonym").isNull)
    .join(citiesWithAllSynsDF, Seq("CityCode"), "left_outer")
    .select(col("CityCode"), col("Name").as("CityName"),nameWithSynSpaceUDF( col("Name"), col("Names")).as("CityNames"))

  citiesFinalDF.filter(col("CityCode") === "AAA").show(2, truncate = false)

  val airWithCityNameDF: DataFrame = airDF.join(citiesFinalDF.select(col("CityCode").as("City"),col("CityName"), col("CityNames")), Seq("City") , "left_outer" ).persist

  airWithCityNameDF.count
  airWithCityNameDF.show(12, truncate = false)
  airWithCityNameDF.printSchema

  val airSynonymsDF: DataFrame = airWithCityNameDF.filter(col("Synonym") === "S")
    .select(col("Code"),col("Name").as("SynonymousName"))
    .groupBy(col("Code")).agg(collect_set(col("SynonymousName")))
    .select(col("Code"), concat_ws(" ", col("collect_set(SynonymousName)")).as("AirPortNames"))

  airSynonymsDF.count

  val airSynJoinedDF: Dataset[Row] = airWithCityNameDF.join(airSynonymsDF, Seq("Code"), "left_outer").filter(col("Synonym").isNull)

  airSynJoinedDF.count
  airSynJoinedDF.show(5, truncate = false)
  airSynJoinedDF.filter(not(col("AirPortNames").isNull)).show(15, truncate = false)

  val parse2UDF: UserDefinedFunction = udf((airPortCode: String, airPortName: String, airPortOtherNames: String, cityCode: String, cityName: String, cityOtherNames: String, country: String) => {
    val regex = """National|Harbour|Memorial|Rgnl|Regional|International|Municipal|Intl|Airport|Arpt|arpt|ARPT|\s+(Intl)\s+""".r
    def clean(input:String) = regex.replaceAllIn(input, " ").trim.replaceAll("( )+", " ").split("( )").toSet.toList.mkString("\"","\",\"", "\"")
    def cleanAll(input: Set[String]) = clean(input.toList.mkString(" "))
    val displayAirportName =  if (Option(airPortOtherNames).exists(_.nonEmpty)) s"""$airPortName ($airPortOtherNames)""" else s"""$airPortName"""
    val tokens = Seq(Option(airPortCode), Option(cityCode), Option(country), Option(airPortName), Option(airPortOtherNames), Option(cityOtherNames), Option(cityCode)).flatten.toSet

    s"""{"name": "$displayAirportName","code": "$airPortCode","country": "$country","cityname": "$cityName","value": "$airPortCode","tokens": [${cleanAll(tokens)}]}"""
  })

  val airportsFinalResult: DataFrame = airSynJoinedDF.withColumn("Output", parse2UDF(col("Code"),col("Name"),col("AirPortNames"),col("City"), col("CityName"), col("CityNames"), col("Country") ))
    .select(col("Output"))//.show(10, false)

  airportsFinalResult.printSchema
  airportsFinalResult.show(10, truncate = false)


  // MAGIC %fs rm -r /FileStore/airportsFinalResult2

  airportsFinalResult
    .coalesce(1)
    .write
    .format("com.databricks.spark.csv")
    .save("/FileStore/airportsFinalResult2")


  // MAGIC %fs ls /FileStore/airportsFinalResult2


}

object Airports {

  def apply(args: Array[String]): Unit = {
    println("Hello")
  }

  def main(args: Array[String]): Unit = {
    apply(args)
  }

}