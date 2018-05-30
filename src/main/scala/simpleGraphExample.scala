import org.apache.spark.graphx.{Edge, Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by betty.schirrmeister on 30/05/2018.
  */
class simpleGraphExample {

  val spark: SparkSession = SparkSession.builder.master("local").getOrCreate


  val users: RDD[(VertexId,(String,String))] = spark.sparkContext.parallelize(Array((3L,("rxin","student")),
    (7L,("jgonzal","postdoc")),(5L,("franklin","prof")),(2L,("istoica","prof"))))
  //create RDD for edges
  val relationships: RDD[Edge[String]] = spark.sparkContext.parallelize(Array(Edge(3L,7L,"collab"),
    Edge(5L,3L,"advisor"),Edge(2L,5L,"colleague"),Edge(5L,7L,"pi")))
  //Define a default user in case there are relationship with missing user
  val defaultUser = ("John Doe","Missing")
  //build the initial graph
  val graph = Graph(users,relationships,defaultUser)
  //show the vertices of the graph
  println("\nThe vertices:")
  graph.vertices.collect.foreach(println)
  //show the edges of the graph
  println("\nThe edge:")
  graph.edges.collect.foreach(println)


}
