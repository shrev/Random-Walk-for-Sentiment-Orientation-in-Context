import org.apache.spark.sql.{SparkSession}

object TopCategories {
  def main(args : Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Task 1").config("spark.master", "local[*]").getOrCreate()
    val data_frame = spark.read.option("header", "true").csv("/Users/ektarita/Documents/CSCI-541_DataMining/Project/Cities/Cleveland.csv")
    val data_frame1 = data_frame.select(data_frame("categories"))

    val d = data_frame1.rdd.flatMap(row=>row.toString().replace("[","")
      .replace("]","").split(","))

    val mapping = d.map(x => (x.trim(),1)).reduceByKey((x,y) => x+y).sortBy(x => -x._2)
    mapping.take(20).foreach(println)
  }
}
