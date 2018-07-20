from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml import Pipeline
def test():
    spark = SparkSession \
        .builder \
        .appName("MovieRe") \
        .getOrCreate()

    #print(spark.sparkContext.getConf().getAll())
    rawData = spark.sparkContext.textFile("hdfs://10.108.218.64:9000/test/ml-100k/u.data") \
        .map(lambda line: line.split("\t")[0:3]) \
        .map(lambda item: (int(item[0]), int(item[1]), float(item[2]))) \
        .toDF(["user", "item", "rating"])
    training, test = rawData.randomSplit([0.8, 0.2])
    als = ALS().setMaxIter(10).setRank(50).setRegParam(0.01)
    pipeline = Pipeline().setStages([als])
    model = pipeline.fit(training)
    ret = model.transform(test)
    ret.select("user", "item", "rating", "prediction").show(100)
    print('yes')

if __name__ == "__main__":
    test()