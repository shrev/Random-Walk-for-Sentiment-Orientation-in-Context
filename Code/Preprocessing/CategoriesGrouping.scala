import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable

object CategoriesGrouping {
  def main(args : Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Task 1").config("spark.master", "local[*]").getOrCreate()
    val data_frame = spark.read.option("header", "true").csv("/Users/ektarita/Documents/CSCI-541_DataMining/Project/Cities/Toronto,ON.csv")
    val data_frame1 = data_frame.select(data_frame("business_id"),data_frame("categories"))

    val category_map = mutable.Map(
      "mexican" -> 0,
      "american (traditional)" -> 0,
      "fast food" -> 0 ,
      "pizza" -> 0,
      "sandwiches" -> 0,
      "bars" -> 0,
      "american (new)" -> 0,
      "italian" -> 0,
      "chinese" -> 0,
      "mediterranean" -> 0,
      "asian" -> 0,
      "thai" -> 0
    )

    category_map.foreach(category => {
      val data = data_frame.filter(r => r.toString().toLowerCase().contains(category._1))
      category_map(category._1) = data.count().toInt
//      println(category._1+".csv")
//
//      data.coalesce(1).write.mode(SaveMode.Overwrite).option("header","true").csv(category._1)
//
//      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//      FileUtil.copyMerge(fs, new Path(category._1), fs, new Path("Toronto,ON_"+category._1+".csv"), true,spark.sparkContext.hadoopConfiguration,null)
    })
    println(category_map)
  }
}
