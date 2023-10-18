// Nhập các thư viện cần thiết: Đầu tiên, chúng ta nhập các thư viện cần thiết từ Spark và GraphX
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// Tạo một SparkContext: SparkContext là một điểm vào chính cho bất kỳ ứng dụng Spark nào. Nó kết nối với một hệ thống chạy Spark và cho phép bạn tạo RDD và phát sóng các biến đến tất cả các nút
val sc = new SparkContext("local", "graphx_example")

// Tạo các đỉnh: Đỉnh biểu diễn các nút trong đồ thị. Mỗi đỉnh có một ID duy nhất và chứa thông tin về tên và tuổi của một người
val vertices: RDD[(VertexId, (String, Int))] = sc.parallelize(Array(
  (1L, ("Chi Hao", 20)),
  (2L, ("Thanh Loi", 21)),
  (3L, ("Tran Chau", 19)),
  (4L, ("Tan Phat", 23)),
  (5L, ("Tan Thong", 24)),
  (6L, ("Ngoc Nhung", 21)),
  (7L, ("Duc Manh", 20)),
  (8L, ("Minh Nhat", 17)),
  (9L, ("Viet Khoa", 18)),
  (10L, ("Tuan Hung", 22)),
  (11L, ("Duc Manh", 20)),
  (12L, ("Ly Cuong", 25))
))

// Tạo các cạnh: Cạnh biểu diễn các mối quan hệ giữa các đỉnh trong đồ thị. Mỗi cạnh chứa thông tin về loại quan hệ

val relationships: RDD[Edge[String]] = sc.parallelize(Array(
  Edge(1L, 2L, "friend"),
  Edge(2L, 3L, "uncle"),
  Edge(3L, 2L, "brother"),
  Edge(3L, 4L, "sister"),
  Edge(1L, 6L, "husband"),
  Edge(4L, 5L, "father"),
  Edge(5L, 6L, "family"),
  Edge(6L, 7L, "follow")
))

// Xác định một giá trị mặc định cho các đỉnh không có dữ liệu: Điều này cần thiết để xử lý trường hợp khi có cạnh trỏ đến một đỉnh không tồn tại
val defaultUser = ("Thai Quang", 26)

// Xây dựng đồ thị: Chúng ta sử dụng hàm Graph để xây dựng đồ thị từ các RDD của đỉnh và cạnh
val graph = Graph(vertices, relationships, defaultUser)

// Hiển thị thông tin về các đỉnh và cạnh trong đồ thị: Chúng ta sử dụng hàm collect để lấy tất cả các phần tử của RDD và sau đó sử dụng hàm foreach để in ra từng phần tử
 graph.vertices.collect.foreach(println)
 graph.edges.collect.foreach(println)

// Tính toán và hiển thị thông tin về bậc của các đỉnh: Bậc của một đỉnh là số lượng cạnh liên kết với nó
val degrees: VertexRDD[Int] = graph.degrees
degrees.collect.foreach(println)

// Sử dụng thuật toán PageRank để xếp hạng các đỉnh: PageRank là một thuật toán được sử dụng rộng rãi trong công nghệ tìm kiếm để xếp hạng các trang web theo mức độ quan trọng
val ranks = graph.pageRank(0.0001).vertices
ranks.collect.foreach(println)
