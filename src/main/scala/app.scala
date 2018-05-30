/**
  * Created by betty.schirrmeister on 27/05/2018.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.sql._


object app {
  def main(args: Array[String]): Unit = {
    //Control log level
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    // Create the Spark context
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import spark.implicits._

    import scala.util.hashing.MurmurHash3
    //create an RDD for edges

    val df_1 = spark.read.format("csv").option("header", "true").load("/Users/betty.schirrmeister/Documents/CODE/Scala/sampleSpark/data/2008.csv")

    //df_1.select($"Origin").show(5)

    val flightsFromTo = df_1.select($"Origin",$"Dest").rdd
    val airportCodes = df_1.flatMap( x => Iterable(x(0).toString, x(1).toString)).rdd
    airportCodes.collect.foreach(println(_))

    val airportVertices: RDD[(VertexId, String)] = airportCodes.distinct().map(x => (MurmurHash3.stringHash(x), x))
    val defaultAirport = ("Missing")

    val flightEdges = flightsFromTo.map(x => ((MurmurHash3.stringHash(x(0).toString),MurmurHash3.stringHash(x(1).toString)), 1)).reduceByKey(_+_).map(x => Edge(x._1._1, x._1._2,x._2))
    //airportVertices.collect.foreach(println(_))
    //flightEdges.collect.foreach(println(_))
    val graph = Graph(airportVertices, flightEdges, defaultAirport)
    graph.persist()

    //now we have some basic graph with airports and flights connecting the airports

    //some basic statistics

    graph.numVertices // 305 airports
    graph.numEdges // 5366 flights

    //top 10 flights
    graph.triplets.sortBy(_.attr, ascending=false)
      .map(triplet => "There were " + triplet.attr.toString + " flights from " + triplet.srcAttr + " to " + triplet.dstAttr + ".")
      //.collect.foreach(println(_))

    //best.collect.foreach(println(_))
    println("hey there! it worked")
    // stop the spark context
    spark.stop
  }


}
