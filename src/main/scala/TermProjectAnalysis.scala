import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, to_date, date_format, concat, from_unixtime, unix_timestamp, date_trunc, round}

//Use DataFrame for data storage.
//Possible to use SQL to query data for relevant information

object TermProjectAnalysis {

	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("Term_Project").getOrCreate()
		val sc = SparkContext.getOrCreate()
		import spark.implicits._
		//Fiscal Year 2017 is from 7/1/2016-6/30/2017 in NYC
	
		//Removes uneeded columns and changes temperature from kelvin to fahenheit and converts it back into a string
		val colsToKeep = Seq("datetime", "New York")
		val temporary_temp = spark.read.format("csv").option("header", "true").load("hdfs://madison:65000/term/data/weather/temperature.csv")
		val temperature = temporary_temp.select(temporary_temp.columns.filter(colName => colsToKeep.contains(colName)).map(colName => new Column(colName)): _*).na.drop().withColumn("New York", (col("New York") - 273.15) * 9 / 5 + 32).withColumn("New York", col("New York").cast("String")).filter(to_date(col("datetime")).between("2016-07-01", "2017-06-30")).withColumnRenamed("New York", "temperature")
	
		val temporary_weather = spark.read.format("csv").option("header", "true").load("hdfs://madison:65000/term/data/weather/weather_description.csv")
		val weather = temporary_weather.select(temporary_weather.columns.filter(col => colsToKeep.contains(col)).map(col => new Column(col)): _*).na.drop().filter(to_date(col("datetime")).between("2016-07-01", "2017-06-30")).withColumnRenamed("New York", "weather")
	
		// Gets NYC data with date format correctly and time formated to be 24 hour timestamp and makes a datetime field that is rounded to match the weather and temperature then joins it with the temperature and weather to get the temperature and weather on the date and time of that ticket being issued
		val data = spark.read.format("csv").option("header", "true").load("hdfs://madison:65000/term/data/parking/*").withColumn("Issue Date", date_format(to_date(col("Issue Date"), "MM/dd/yyyy"), "yyyy-MM-dd")).withColumn("Violation Time", col("Violation Time").cast("String")).withColumn("Violation Time", concat(col("Violation Time"), lit("M"))).withColumn("Violation Time", from_unixtime(unix_timestamp(col("Violation Time"), "hhmmaa"), "HH:mm:ss")).withColumn("Violation Time", date_format(date_trunc("hour", col("Violation Time")), "HH:mm:ss")).withColumn("datetime", concat(col("Issue Date"), lit(" "), col("Violation Time"))).join(temperature, Seq("datetime")).join(weather, Seq("datetime")).drop("Street Code1", "Street Code2", "Street Code3", "Vechicle Expiration Date", "Time First Observed", "Violation In Front of Or Opposite", "House Number", "Intersecting Street", "Date First Observed", "law Section", "Days Parking In Effect", "From Hours In Effect", "To Hours In Effect", "No Standing or Stopping Violation", "Hydrant Violation", "Double Parking Violation", "Feet From Curb", "Violation Post Code")
    
		// Top ticketing weather
		data.groupBy("weather").count().coalesce(1).write.csv("/term/output/weatherTickets/")
    
		//Top temperature
		data.withColumn("temperature", round(col("temperature"), -1)).groupBy("temperature").count().coalesce(1).write.csv("/term/output/temperatureTickets/")
    
		//Top body types ticketed
		data.groupBy("Vehicle Body Type").count().coalesce(1).write.csv("/term/output/bodyTypes/")
    
		// Top times ticketed
		data.groupBy("Violation Time").count().coalesce(1).write.csv("/term/output/timesTicketed/")
    
		// Top precincts ticketing
		data.groupBy("Issuer Precinct").count().coalesce(1).write.csv("/term/output/precinctTickets/")
    
		// Top ticketed streets
		data.groupBy("Street Name").count().coalesce(1).write.csv("/term/output/streetsTicketed/")
    
		// precinctViolations is the amount of tickets given out in a precinct area
		data.groupBy("Violation Precinct").count().coalesce(1).write.csv("/term/output/precinctViolations/")
    
		// Top ticketed registration state in NYC
		data.groupBy("Registration State").count().coalesce(1).write.csv("/term/output/regStateTickets/")
    
		// Top Violation Code
		data.groupBy("Violation Code").count().coalesce(1).write.csv("/term/output/violationCode/")
    
		// Top Violation Code during specific weather
		data.groupBy("Violation Code", "weather").count().coalesce(1).write.csv("/term/output/violationWeather/")
    
		// Top Violation on Streets
		data.groupBy("Street Name", "Violation Code").count().coalesce(1).write.csv("/term/output/violationStreet/")
    
		// Violation precinct vs issuer precinct
		data.filter(data("Violation Precinct") !== data("Issuer Precinct")).filter(data("Issuer Precinct") !== "0").groupBy("Violation Precinct", "Issuer Precinct").count().coalesce(1).write.csv("/term/output/violationIssuerPrecinct/")
	}
	
}
