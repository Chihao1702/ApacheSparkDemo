import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib._
val verArray = Array(
(1L, ("Philadelphia", 1580863)),
(2L, ("Baltimore", 620961)),
(3L, ("Harrisburg", 49528)),
(4L, ("Wilmington", 70851)),
(5L, ("New York", 8175133)),
(6L, ("Scranton", 76089)))
 
verArray: Array[(Long, (String, Int))] = Array((1,(Philadelphia,1580863)), (2,(Baltimore,620961)), (3,(Harrisburg,49528)), (4,(Wilmington,70851)), (5,(New York,8175133)), (6,(Scranton,76089)))
val edgeArray = Array(
Edge(2L, 3L, 113),
Edge(2L, 4L, 106),
Edge(3L, 4L, 128),
Edge(3L, 5L, 248),
Edge(3L, 6L, 162),
Edge(4L, 1L, 39),
Edge(1L, 6L, 168),
Edge(1L, 5L, 130),
Edge(5L, 6L, 159))
edgeArray: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(2,3,113), Edge(2,4,106), Edge(3,4,128), Edge(3,5,248), Edge(3,6,162), Edge(4,1,39), Edge(1,6,168), Edge(1,5,130), Edge(5,6,159))
val verRDD = sc.parallelize(verArray)
val edgeRDD = sc.parallelize(edgeArray)
val graph = Graph(verRDD, edgeRDD)
