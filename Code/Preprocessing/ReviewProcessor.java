import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;

public class ReviewProcessor {


    public static void main(String args[]) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader("/home/shreyav/Documents/Data Mining Project/ids.txt"));
        String line = null;
        while((line=br.readLine())!=null) {
            filterByBusinessId(line.trim());
        }

    }


    public static void filterByBusinessId(String businessId) {

        int count = 0;
        int err = 0;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader("/home/shreyav/Documents/Data Mining Project/yelp_dataset/yelp_academic_dataset_review.json");
            FileReader fileReader_tips = new FileReader("/home/shreyav/Documents/Data Mining Project/yelp_dataset/yelp_academic_dataset_tip.json");
            FileWriter file = new FileWriter("/home/shreyav/Documents/Data Mining Project/" + businessId + ".json");


            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            BufferedReader bufferedReader_tips = new BufferedReader(fileReader_tips);

            String line;


            while ((line = bufferedReader.readLine()) != null) {
                JSONObject obj = new JSONObject(line);
                if (obj.has("business_id") && obj.get("business_id") != JSONObject.NULL && (obj.getString("business_id").equals(businessId))) {
                    file.write(obj.toString() + "\n");
                }

                System.out.println(count++);
            }

            while ((line = bufferedReader_tips.readLine()) != null) {
                JSONObject obj = new JSONObject(line);
                if (obj.has("business_id") && obj.get("business_id") != JSONObject.NULL && (obj.getString("business_id").equals(businessId))) {
                    file.write(obj.toString() + "\n");
                }

                System.out.println(count++);
            }
            // Always close files.
            file.close();
            bufferedReader.close();
            bufferedReader_tips.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException ex) {
            ex.printStackTrace();
            System.out.println(err++);
        }
    }

    public static void filterTipsByBusinessId(String businessId) {

        int count = 0;
        int err = 0;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader("/home/shreyav/Documents/Data Mining Project/yelp_dataset/yelp_academic_dataset_tip.json");
            FileWriter file = new FileWriter("/home/shreyav/Documents/Data Mining Project/" + businessId + "_tips.json");


            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line;


            while ((line = bufferedReader.readLine()) != null) {
                JSONObject obj = new JSONObject(line);
                if (obj.has("business_id") && obj.get("business_id") != JSONObject.NULL && (obj.getString("business_id").equals(businessId))) {
                    file.write(obj.toString() + "\n");
                }

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
        }
    }



}
