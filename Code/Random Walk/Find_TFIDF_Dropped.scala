import java.io.{File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import util.control.Breaks._

object Find_TFIDF {
  def main(args: Array[String]) : Unit = {

    val spark_config = new SparkConf().setAppName("Tf-Idf").setMaster("local[*]")
    val spark_context = new SparkContext(spark_config)

    val data = spark_context.textFile("/Users/ektarita/Documents/CSCI-541_DataMining/Project/Processed_Reviews/s4fNTcuW6DGfiAagPMlQgw_processed.csv")
      .map(line => line.split(",")).mapPartitionsWithIndex{(index, review) => if (index == 0) review.drop(1) else review}

    val split_data = data.map(row=> (row(0), row(1).replace(".","").replaceAll(" +"," ").split(" ")))

    val reviews = split_data.mapValues(row=> row.toList)
    reviews.foreach(println)

    //Calculate tf
    val tf = mutable.HashMap[(String, String), Double]()

    reviews.collect().foreach(r => {
      val user_id = r._1
      val no_of_terms_in_doc = r._2.size.toDouble
      val review_words = r._2.groupBy(identity).mapValues(v => v.size/no_of_terms_in_doc)
      review_words.foreach(word => {
        tf((word._1,user_id)) = word._2
      })
    })

    //Calculate idf
    val no_of_docs_with_term = mutable.HashMap[String,Int]()

    val total_no_of_docs = reviews.count()

    val idf = mutable.HashMap[String, Double]()

    tf.foreach(line => {
      val term = line._1._1
      if(no_of_docs_with_term.contains(term)){
        no_of_docs_with_term(term) += 1
      }
      else{
        no_of_docs_with_term(term) = 1
      }
    })

    for (elem <- no_of_docs_with_term) {
      idf(elem._1) = Math.log(total_no_of_docs/elem._2.toDouble)
    }

    //Calculate tf-idf
    val tf_idf = tf.map(elem =>{
      val word = elem._1._1
      val user_id = elem._1._2
      val tf_val = elem._2
      val idf_val = idf(word)
      ((word, user_id), tf_val * idf_val)
    })

    //Calculate edges

    val edges = mutable.HashMap[(String, String), Double]()

    val window_size = 3

    split_data.collect().foreach(row => {
      val user_id = row._1
      val review_arr = row._2

      for (i <- 0 until review_arr.size){
        var count = 1
        val word1 = review_arr(i)
        breakable{
          while(count<=window_size){
            if(i+count<review_arr.size){
              val word2 = review_arr(i+count)
              if(!edges.contains((word1,word2))){
                edges((word1,word2)) = tf_idf((word1,user_id)) *tf_idf((word2,user_id))
              }
              else{
                edges((word1,word2)) += tf_idf((word1,user_id)) *tf_idf((word2,user_id))
              }
              edges((word2,word1)) = edges((word1,word2))
              count += 1
            }
            else{
              break
            }
          }
        }
      }
    })

    val fw = new FileWriter(new File("/Users/ektarita/Documents/CSCI-541_DataMining/Project/TFIDF/s4fNTcuW6DGfiAagPMlQgw_tfidf.txt"))

    edges.foreach(row => {
      val word1 = row._1._1
      val word2 = row._1._2
      val weight = row._2

      fw.write(word1 +" "+ word2+" {'weight': "+weight+"}\n")
    })
    
    fw.close()
  }
}
