import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object JaccardSimilarity{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.format("csv").option("header", "true").load("/Users/ektarita/Documents/CSCI-541_DataMining/Project/Cusines/Toronto-Thai.csv").toDF()
    println(df.count())

    val lines = df.toDF().filter(col("attributes_Ambience").isNotNull).filter(col("attributes_BusinessParking").isNotNull).filter(col("attributes_GoodForMeal").isNotNull).filter(col("business_id").isNotNull).toDF()

    val dataframe = lines.select("business_id","attributes_AcceptsInsurance" , "attributes_AgesAllowed", "attributes_Alcohol", "attributes_Ambience" , "attributes_BYOB", "attributes_BYOBCorkage", "attributes_BestNights" , "attributes_BikeParking", "attributes_BusinessAcceptsBitcoin" , "attributes_BusinessAcceptsCreditCards", "attributes_BusinessParking", "attributes_ByAppointmentOnly", "attributes_Caters", "attributes_CoatCheck", "attributes_Corkage", "attributes_DietaryRestrictions", "attributes_DogsAllowed", "attributes_DriveThru", "attributes_GoodForDancing", "attributes_GoodForKids", "attributes_GoodForMeal", "attributes_HappyHour", "attributes_HasTV", "attributes_Music", "attributes_NoiseLevel", "attributes_Open24Hours", "attributes_OutdoorSeating", "attributes_RestaurantsAttire", "attributes_RestaurantsCounterService", "attributes_RestaurantsDelivery", "attributes_RestaurantsGoodForGroups", "attributes_RestaurantsPriceRange2", "attributes_RestaurantsReservations", "attributes_RestaurantsTableService", "attributes_RestaurantsTakeOut", "attributes_Smoking", "attributes_WheelchairAccessible", "attributes_WiFi")

    var starRDD = df.toDF().select("business_id","stars", "review_count").sort(asc("stars"),desc("review_count")).rdd
    //df.na.replace(Seq("attributes_AcceptsInsurance" , "attributes_AgesAllowed"),Map(""-> null)).na.fill("-1", Seq("attributes_AcceptsInsurance" , "attributes_AgesAllowed")).show(false)


    //{'romantic': False, 'intimate': False, 'classy': False, 'hipster': False, 'touristy': False, 'trendy': False, 'upscale': False, 'casual': True}

    dataframe.withColumn("attributes_Ambience", split(col("attributes_Ambience"), "\\,")).select(
      col("attributes_Ambience" replace("{'romantic': ", "")).getItem(0).as("romantic"),
      col("attributes_Ambience" replace(" 'intimate': ", "")).getItem(1).as("intimate"),
      col("attributes_Ambience" replace(" 'classy': ", "")).getItem(2).as("classy"),
      col("attributes_Ambience" replace(" 'hipster': ", "")).getItem(3).as("hipster"),
      col("attributes_Ambience" replace(" 'touristy': ", "")).getItem(4).as("touristy"),
      col("attributes_Ambience" replace(" 'trendy': ", "")).getItem(5).as("trendy"),
      col("attributes_Ambience" replace(" 'upscale': ", "")).getItem(6).as("upscale"),
      col("attributes_Ambience" replace(" 'casual': ", "") replace("}", "")).getItem(7).as("casual")
    ).drop("attributes_Ambience").show()



    //{'garage': False, 'street': True, 'validated': False, 'lot': False, 'valet': False}

    dataframe.withColumn("attributes_BusinessParking", split(col("attributes_BusinessParking"), "\\,")).select(
      col("attributes_BusinessParking" replace("{'garage': ", "")).getItem(0).as("garage"),
      col("attributes_BusinessParking" replace(" 'street': ", "")).getItem(1).as("street"),
      col("attributes_BusinessParking" replace(" 'validated': ", "")).getItem(2).as("validated"),
      col("attributes_BusinessParking" replace(" 'lot': ", "")).getItem(3).as("lot"),
      col("attributes_BusinessParking" replace(" 'valet': ", "")).getItem(4).as("valet")
    ).drop("attributes_BusinessParking").show()



    //{'dessert': False, 'latenight': False, 'lunch': True, 'dinner': False, 'breakfast': False, 'brunch': False}

    dataframe.withColumn("attributes_GoodForMeal", split(col("attributes_GoodForMeal"), "\\,")).select(
      col("attributes_GoodForMeal" replace("'dessert': ", "")).getItem(0).as("dessert"),
      col("attributes_GoodForMeal" replace(" 'latenight': ", "")).getItem(1).as("latenight"),
      col("attributes_GoodForMeal" replace(" 'lunch': ", "")).getItem(2).as("lunch"),
      col("attributes_GoodForMeal" replace(" 'dinner': ", "")).getItem(3).as("dinner"),
      col("attributes_GoodForMeal" replace(" 'breakfast': ", "")).getItem(4).as("breakfast"),
      col("attributes_GoodForMeal" replace(" 'brunch': ", "")).getItem(4).as("brunch")
    ).drop("attributes_GoodForMeal").show()



    println(lines.count())
    for (line <- dataframe){
      println("abc "+line)

    }

    val columnNames = dataframe.columns

    val f_final = dataframe.select( columnNames.head, columnNames.tail: _*).rdd
    println(f_final.count())

    val hashmap = scala.collection.mutable.HashMap[String, String]()

    f_final.collect().foreach(line => {
      val key = line(0).toString()
      hashmap(key) = line.toString()
      println(line.toString())
    })

    val worstRestaurant = starRDD.first()
    val worstRestaurantId = worstRestaurant.get(0).toString
    val worstRating = worstRestaurant.get(1).toString.toDouble
    val r1_attr = hashmap(worstRestaurantId).split(",")

    println("worst: "+ worstRestaurantId)
    val otherRestaurants = starRDD.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    def JaccardSimilarity(attr1 : Array[String], attr2 : Array[String]) : Double = {

      var similarity = 0.0
      var intersectCount = 0
      var count1 = 0
      var count2 = 0
      for( i <- attr1.indices){
        if(attr1(i)!=null && attr2(i)!=null && attr1(i).equals(attr2(i))){
          intersectCount += 1
        }
        if(attr1(i)!=null){
          count1 += 1
        }
        if(attr2(i)!=null){
          count2 += 1
        }
      }
      if(intersectCount!=0){
        similarity = intersectCount.toDouble /(count1 + count2 - intersectCount -2)
      }
      similarity
    }

    val similarity = otherRestaurants.map(row =>{
      val rest_id = row(0).toString
      val rating = row(1).toString.toDouble

      var sim = -1.0
      if(rating >= worstRating+1){
        val r2_attr = hashmap(rest_id).split(",")
        sim = JaccardSimilarity(r1_attr,r2_attr)
      }
      (rest_id,sim)
    }).sortBy(row => -row._2).take(1)

    similarity.foreach(println)
  }
}
