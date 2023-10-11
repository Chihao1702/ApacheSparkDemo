// Import thư viện SQL spark
from pyspark.sql import SparkSession

// Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Spark SQL Example") \
    .getOrCreate()

// Tạo DataFrame từ dữ liệu CSV
df = spark.read.csv("example.csv", header=True, inferSchema=True)

// Tạo một bảng tạm thời từ DataFrame, bảng này có thể được truy vấn bằng SQL
df.createOrReplaceTempView("people")

// Thực hiện truy vấn SQL 
// Tìm tên những người dưới nhỏ hơn hoặc bằng 22 tuổi
result = spark.sql("SELECT name, age FROM people WHERE age <= 22")

// Hiển thị kết quả
result.show()
