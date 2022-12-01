package com.sidSparkScala.RatingsCounter
import java.text.SimpleDateFormat
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{col, sum, to_timestamp, udf}
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import org.mongodb.scala.model.Indexes
import org.mongodb.scala.model.Filters
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.mongodb.scala.model.Filters.geoWithinCenter

object mongoRead {
  //using MongoSpark, by collecting tweets and filtering them spatio-temporally using dataframe apis.
  def frequencyDf(spark: SparkSession,w: String, r: Double, lon: Double, lat: Double,start: String, end: String ): Any = {
    // load all documents
    val df = MongoSpark.load(spark)
    // filtering document
    val dataframe = df.filter(col("created_at").gt(start) && col("created_at").lt(end) &&
       // to measure the distance between point and the centre
      //  square_dist = (center_x - x) ^ 2 + (center_y - y) ^ 2
      //  return square_dist <= radius ^ 2
        (col("coordinates")("coordinates")(0)- lon) * (col("coordinates")("coordinates")(0)- lon) +
          (col("coordinates")("coordinates")(1)- lat) * (col("coordinates")("coordinates")(1) - lat) <
          Math.pow(r, 2)
    )
    //calculate the number of word frequency in the documents
    val counter = (text: String) => {
      var c = 0
      text.split(" ").foreach(word => if(word == w)
        c = c + 1)
      // return count (the number of word frequency in the documents)
      c
    }
    // create the user define function to use it on DataFrame
    val countWord = udf(counter)
    // create a new column called 'wordCount'
    val freq = dataframe.withColumn("wordCount",countWord(col("text")))
    //Compute sum of the number aggregates by specifying a map from column name.
    freq.agg(sum("wordCount")).collect()(0)(0)
  }
  //using mongodb library by sending a normal mongoDB query to filter by time and space
  def frequencyQuery(collection: MongoCollection[org.bson.Document],w: String, r:Float, lon: Double, lat: Double, start: String, end: String ): Int ={
    // to count number of repeated KeyWord
    val dateF = new SimpleDateFormat("yyyy-MM-dd")
    // date formatter to convert from dateF to "yyyy-MM-dd"
    val startF = dateF.parse(start)
    val endF = dateF.parse(end)
    //get all tweets in the longitude, latitude ,radius ,start and end Date
    val coll = collection.find(Filters.and(Filters.and(
      Filters.gt("created_at", startF),
      Filters.lt("created_at", endF)),
      //geoWithinCenter: Selects all documents containing a field with grid coordinates data that exist entirely within the specified circle
      //iterator: a way to access the elements of a collection one by one ...then convert to list
      geoWithinCenter("coordinates.coordinates", lon,lat, r))).iterator().toList
//calculate the number of word frequency in the documents
var c = 0
    val countW = coll.map(x => x.getString("text"))
    .flatMap(x => x.split(" "))
    countW.foreach( word =>{if(word==w)
        c = c + 1
    })
    // return count(the number of word frequency in the documents)
    c
  }

  def main(args: Array[String]): Unit = {
    //control log settings in Spark and stop INFO messages
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    //user's input
    println("Enter the word : ") //life
    val w = scala.io.StdIn.readLine()
    println("Enter the radius : ") //1000
    val r = scala.io.StdIn.readFloat()
    println("Enter the longitude : ") //34.14628356
    val lon = scala.io.StdIn.readDouble()
    println("Enter the latitude : ") //-118.10041174
    val lat = scala.io.StdIn.readDouble()
    println("Enter the starting epoc time : ") //2013-01-01
    val start = scala.io.StdIn.readLine()
    println("Enter the ending epoc time: ")  //2015-03-07
    val end = scala.io.StdIn.readLine()
    //create spark session
    val spark = SparkSession.builder()
      .appName("tweets").master("local[*]")
      //database name : mongoDBT                collection name : tweets
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mongoDBT.3011")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/mongoDBT.3011")
      .getOrCreate()
    //create spark context
    val sc = spark.sparkContext
    //read and load the json file(boulder_flood_geolocated_tweets.json)
    val fileDf = spark.read.format("json")
      .option("sep", ",").option("header", "true")
      .load("src\\main\\boulder_flood_geolocated_tweets.json")
    //covert date from string to Date object
    val formatt = (timeStamp: String) => {
      // define a formatter to convert "E MMM dd HH:mm:ss Z yyyy" to new format
      val formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy")
      // convert to date type
      val formatted = formatter.parse(timeStamp)
      // convert to the new format
      val newFormat = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(formatted)
      newFormat
    }
    import spark.implicits._
    // create the user define function to use it on DataFrame
    val udff = udf(formatt)
    // change format date in column & return the new format
    val new_df = fileDf.withColumn("created_at", udff('created_at))
    // convert the date column to timestamp type
    val dff = new_df.withColumn("created_at", to_timestamp($"created_at", "MM-dd-yyyy HH:mm:ss"))
    // create configuration to write on mongoDB
    val writeConf = WriteConfig(Map("collection" -> "3011",
      "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
    // save the dff in the mongoDB
    MongoSpark.save(dff, writeConf)
    // connect to mongo
    val mongoClient: MongoClient = new MongoClient("localhost", 27017)
    val database = mongoClient.getDatabase("mongoDBT")
    val collection = database.getCollection("3011")
    //Indexing the geo-coordinates of tweets to ensure a fast spatial-based retrieval
    // indexing of geo location
    collection.createIndex(Indexes.geo2dsphere("coordinates.coordinates"))
    // indexing of datetime
    collection.createIndex(Indexes.ascending("created_at"))
    //call frequencyDf function
    println("using dataFrame APIs:")
    val wordN: Any= frequencyDf(spark, w, r, lon, lat, start, end)
    println(s"The word '$w' occurrences $wordN time")   //14
    //call frequencyQuery function
    print("using mongoDB query: \n")
    val wordC: Int = frequencyQuery(collection, w, r, lon, lat, start, end)
    println(s"The word '$w' occurrences $wordC time")   //14

  }
}
