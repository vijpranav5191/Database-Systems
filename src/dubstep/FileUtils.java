package dubstep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class FileUtils {
	
	public static ArrayList<String> getDBContents(String tableName){
		String csvFile = "/Users/pranavvij/Desktop/data/" + tableName.toLowerCase() + ".dat";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        ArrayList<String> list = new ArrayList<>();
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                list.add(line);
            }
        } catch (FileNotFoundException e) {
        	e.printStackTrace();
      		System.out.println("Error 1 " + tableName);
      		return null;
        } catch (IOException e) {
        	e.printStackTrace();
      		System.out.println("Error 2 " + tableName);
      		return null;
	    } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
	}
}