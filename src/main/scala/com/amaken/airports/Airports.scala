package com.amaken.airports

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


class Airports {

  //Create or get a SparkSession
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  //Create an implicit sqlContext
  implicit val sqlContext: SQLContext = spark.sqlContext

  /***
    * Load a CSV file from disk
    * @param path The path to load from
    * @param options A map of Key-Value options to override the existing options inside the definition
    * @param sqlContext An implicit sqlContext to use for reading the files
    * @return A DataFrame object representing the input file
    */
  def loadCSV(path: String, options: Map[String,String] = Map.empty[String,String])(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext
      .read
      .format(options.getOrElse("format", "csv"))
      .option("header", options.getOrElse("header", "true"))
      .option("inferSchema", options.getOrElse("inferSchema", "true"))
      .option("delimiter", options.getOrElse("delimiter", ","))
      .load(path)

  }


  val nameWithSynUDF: UserDefinedFunction = udf((name: String, sName: String) => { if (Option(sName).exists(_.nonEmpty)) s"""$name ($sName)""" else s"""$name""" })

  val nameWithSynSpaceUDF: UserDefinedFunction = udf((name: String, sName: String) => { if (Option(sName).exists(_.nonEmpty)) s"""$name $sName""" else s"""$name""" })

  val arrayToStringUDF: UserDefinedFunction = udf((input: Array[String]) => { input.toList.mkString(",") })


  /***
    * Entry point
    */
  def process(): Unit = {

    // Load Input :: Cities data set
    val citiesInputDF: DataFrame =  loadCSV("data/input/cities.csv")
    citiesInputDF.printSchema()

    // Load Input :: Airports data set
    val airportsInputDF: DataFrame = loadCSV("data/input/airports.csv")
    airportsInputDF.printSchema()

    // Get the records which are indicated by an "S" flag (Synonyms).
    val citiesSynonymsDF: DataFrame = citiesInputDF
      .filter(col("Synonym") === "S")
      .select(col("CityCode"),col("Name").as("citySynonymousName"))

    // Collect synonyms in to a list based on CityCode as key
    val citiesWithAllSynonymsDF: DataFrame = citiesSynonymsDF
      .groupBy(col("CityCode"))
      .agg(collect_set(col("citySynonymousName")).as("citySynonymousName"))
      .select(col("CityCode"), concat_ws(" ", col("citySynonymousName")).as("Names"))

    // Join the cities which are not synonyms with the collected set of city synonyms
    // to get a city and a list of synonyms in the same record.
    val citiesFinalDF: DataFrame = citiesInputDF
      .filter(col("Synonym").isNull)
      .join(citiesWithAllSynonymsDF, Seq("CityCode"), "left_outer")
      .select(col("CityCode"), col("Name").as("CityName"),nameWithSynSpaceUDF( col("Name"), col("Names")).as("CityNames"))


    // Filter airports with type 1, 2 & 3 (We are interested in major airports only)
    val airportsFilteredDF: Dataset[Row] = airportsInputDF
      .filter( col("Type") === 1 || col("Type") === 2 || col("Type") === 3 )
      .persist

    // Proceed with only the required columns
    val airportsDF: DataFrame = airportsFilteredDF
      .select(col("Code"),col("Name"),col("City"),col("Country"),col("Synonym"))
      .persist


    // Join the processed cities and the airports based on the 3 character city code
    val airportsWithCityNameDF: DataFrame = airportsDF
      .join(citiesFinalDF.select(col("CityCode").as("City"),col("CityName"), col("CityNames")), Seq("City") , "left_outer" )
      .persist

    //Collect the airport synonyms in a list based on the "S" flag similar to the cities
    val airportSynonymsDF: DataFrame = airportsWithCityNameDF
      .filter(col("Synonym") === "S")
      .select(col("Code"),col("Name").as("SynonymousName"))
      .groupBy(col("Code"))
      .agg(collect_set(col("SynonymousName")))
      .select(col("Code"), concat_ws(" ", col("collect_set(SynonymousName)")).as("AirPortNames"))

    // Similarly join the main list with synonyms list based on airport code
    val airportSynonymsJoinedDF: Dataset[Row] = airportsWithCityNameDF
      .join(airportSynonymsDF, Seq("Code"), "left_outer")
      .filter(col("Synonym").isNull)

    /***
      * A UDF to generate the output as a single string.
      * Collecting all the interesting parts into the tokens field in the process.
      */
    val parse2UDF: UserDefinedFunction = udf((airPortCode: String, airPortName: String, airPortOtherNames: String, cityCode: String, cityName: String, cityOtherNames: String, country: String) => {

      // We need to remove these words from the output to stop matching MUN to all the Municipal airports and only match with MUNICH city airports for example.
      val regex = """National|Harbour|Memorial|Rgnl|Regional|International|Municipal|Intl|Airport|Arpt|arpt|ARPT|\s+(Intl)\s+""".r

      def clean(input:String) = regex
        .replaceAllIn(input, " ")
        .trim
        .replaceAll("( )+", " ")
        .split("( )")
        .toSet
        .toList
        .mkString("\"","\",\"", "\"")

      def cleanAll(input: Set[String]) = clean(input.toList.mkString(" "))

      // Format for output. Main name with other synonyms inside a bracket.
      val displayAirportName =  if (Option(airPortOtherNames).exists(_.nonEmpty)) s"""$airPortName ($airPortOtherNames)""" else s"""$airPortName"""

      // Collect all the interesting parts of the airport name, airport name synonyms and city name and synonyms in to a list
      val tokens = Seq(Option(airPortCode), Option(cityCode), Option(country), Option(airPortName), Option(airPortOtherNames), Option(cityOtherNames), Option(cityCode)).flatten.toSet

      // Output to match the desired requirement
      s"""{"name": "$displayAirportName","code": "$airPortCode","country": "$country","cityname": "$cityName","value": "$airPortCode","tokens": [${cleanAll(tokens)}]}"""
    })

    // Generate output
    val airportsFinalResult: DataFrame = airportSynonymsJoinedDF
      .withColumn("Output", parse2UDF(col("Code"),col("Name"),col("AirPortNames"),col("City"), col("CityName"), col("CityNames"), col("Country") ))
      .select(col("Output"))

    // Write output to a file. Coalesce(1) to write to a single file.
    airportsFinalResult
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .save("data/output/airports.json")

  }
}

object Airports {

  def apply(): Unit = {
    (new Airports).process()
  }

  def main(args: Array[String]): Unit = {
    apply()
  }

}