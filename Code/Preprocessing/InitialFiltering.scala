import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
object ProjectUpdate1{
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()


    val df = spark.read.format("csv").option("header", "true").load("businesses_restaurants.csv")
    var lines = df.toDF()


    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("project.csv")))
    val csvWriter = new CSVWriter(writer)






    var counts = lines.count()
    var c = lines.groupBy("City" ).count().orderBy("City").collect()




    csvWriter.writeNext(Array("Total", counts.toString))


    for (line <- c){
      csvWriter.writeNext(Array(line(0).toString(), line(1).toString))
    }
    writer.close()


  }
}


<!--stackedit_data:
eyJoaXN0b3J5IjpbNTQ0MTkyOTk4XX0=
-->