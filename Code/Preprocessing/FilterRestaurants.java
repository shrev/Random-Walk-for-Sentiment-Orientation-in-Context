

import org.json.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Locale;

public class FilterRestaurants {

    public static void main(String args[]) {

    filterJson();

    }

    public static void filterJson() {


        //Read JSON file
        //String text = new String(Files.readAllBytes(Paths.get("/home/shreyav/Documents/Data Mining Project/dummy.json")), StandardCharsets.UTF_8);

        int count = 0;
        int err=0;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader("/home/shreyav/Documents/Data Mining Project/yelp_dataset/yelp_academic_dataset_business.json");
            FileWriter file = new FileWriter("/home/shreyav/Documents/Data Mining Project/businesses_restaurants.json");


            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line;


            while ((line = bufferedReader.readLine()) != null) {
                JSONObject obj = new JSONObject(line);
                if (obj.has("category") && obj.get("categories")!= JSONObject.NULL && (obj.getString("category").contains("Restaurant")
                        || obj.getString("category").contains("restaurant")
                        || obj.getString("category").contains("Restaurants") || obj.getString("category").contains("Food")
                        || obj.getString("category").contains("retaurants"))) {
                    file.write(obj.toString()+"\n");
                }

                else if (obj.has("attributes") && obj.get("attributes")!=JSONObject.NULL) {
                    //System.out.println(obj.get("attributes").getClass());
                    JSONObject attribute = obj.getJSONObject("attributes");
                    if (attribute.has("RestaurantsTakeOut")) {
                        file.write(obj.toString()+"\n");
                    }
                }
                else
                    continue;
                System.out.println(count++);
            }
            // Always close files.
            file.close();
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException ex) {
            ex.printStackTrace();
            System.out.println(err++);
        } finally {

        }
    }


}
