from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession

# Tạo một phiên Spark
spark = SparkSession.builder.appName('MLlib Persistence Demo').getOrCreate()

# Chuẩn bị dữ liệu đào tạo
training = spark.createDataFrame([
    (1.0, [0.0, 1.1, 0.1]),
    (0.0, [2.0, 1.0, -1.0]),
    (0.0, [2.0, 1.3, 1.0]),
    (1.0, [0.0, 1.2, -0.5])], ["label", "features"])

# Tạo một thể hiện LogisticRegression
lr = LogisticRegression(maxIter=10, regParam=0.01)

# Fit mô hình
model = lr.fit(training)

# Lưu mô hình
model.save("/tmp/model")

# Tải mô hình
from pyspark.ml.classification import LogisticRegressionModel
model2 = LogisticRegressionModel.load("/tmp/model")

# Kiểm tra xem các hệ số của mô hình có giống nhau không
assert model.coefficients == model2.coefficients

# Dừng phiên Spark
spark.stop()
