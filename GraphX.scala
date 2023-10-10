import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib._
val verArray = Array(
(1L, ("TP HCM", 8899866)),
(2L, ("Da Nang", 1134000)),
(3L, ("Nha Trang", 422601)),
(4L, ("Ha Noi", 8330834)),
(5L, ("Quang Nam", 1530500)),
(6L, ("Hai Phong", 2029000)))
 
verArray: Array[(Long, (String, Int))] = Array((1,(TP HCM,8899866)), (2,(Da Nang,1134000)), (3,(Nha Trang,422601)), (4,(Ha Noi,8330834)), (5,(Quang Nam, 1530500)), (6,(Hai Phong, 2029000)))
val edgeArray = Array(
Edge(2L, 3L, 500),
Edge(2L, 4L, 763),
Edge(3L, 4L, 1290),
Edge(3L, 5L, 342),
Edge(3L, 6L, 1350),
Edge(4L, 1L, 1723),
Edge(1L, 6L, 1700),
Edge(1L, 5L, 850),
Edge(5L, 6L, 910))
edgeArray: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(2,3,500), Edge(2,4,763), Edge(3,4,1290), Edge(3,5,342), Edge(3,6,1350), Edge(4,1,1723), Edge(1,6,1700), Edge(1,5,850), Edge(5,6,910))
val verRDD = sc.parallelize(verArray)
val edgeRDD = sc.parallelize(edgeArray)
val graph = Graph(verRDD, edgeRDD)
