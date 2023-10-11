// Import thư viện SQL spark
import org.apache.spark.sql.SparkSession

// Khởi tạo SparkSession
val spark = SparkSession.builder()
  .appName("Spark SQL Example")
  .getOrCreate()

// Tạo DataFrame từ dữ liệu CSV
val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("example.csv")

// Tạo một bảng tạm thời từ DataFrame, bảng này có thể được truy vấn bằng SQL
df.createOrReplaceTempView("people")

// Thực hiện truy vấn SQL 
// Tìm tên những người dưới 22 tuổi
val result = spark.sql("SELECT name, age FROM people WHERE age <= 22")

// Hiển thị kết quả
result.show()
