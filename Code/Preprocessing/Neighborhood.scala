import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
object ProjectNeighborhood{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val df = spark.read.format("csv").option("header", "true").load("Torontothai.csv")
    var lines = df.toDF().filter(col("review_count") > 50).toDF()

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("project_2.csv")))
    val csvWriter = new CSVWriter(writer)

    var c = lines.groupBy("neighborhood" ).count().orderBy(desc("count")).collect()

    if (c(0)(0)!= null && c(0)(0).toString.equalsIgnoreCase("null") )  {
      var final_df = lines.filter(col("neighborhood") === (c(0)(0)))
      println(final_df.count())
      final_df.toDF()
      final_df
        .repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .save("mydata.csv")
    }

    else{
      var final_df = lines.filter(col("neighborhood") === (c(1)(0)))
      println(final_df.count())
      final_df.toDF()
      final_df
        .repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .save("mydata.csv")
    }


    for (line <- c){

          println((line(0),line(1)))

    }

  }
}


