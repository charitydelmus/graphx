import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

case class harbour(Route:String, From:String, To:String, Trip_no:Int)

def parseHarbour(str: String): harbour = {
    val line = str.split(",");
    harbour(line(1), line(2), line(3), line(4).toInt)}

var textRDD = sc.textFile("hadoop_mirrored.csv")

val header = textRDD.first()

textRDD = textRDD.filter(row => row != header)

val harbourRDD = textRDD.map(parseHarbour).cache()

val harbours = harbourRDD.flatMap(journey => Seq((journey.Route, journey.From), (journey.Trip_no, journey.To))).distinct

harbours.take(1)

val routes = harbourRDD.map(route => ((route.From, route.To), route.Trip_no)).distinct

routes.take(2)

val graph = Graph(harbours, edges, nowhere)

graph.edges.take(2)