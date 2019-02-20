package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import interfaces.OnTupleGetListener;

public class FileUtils {
	
	public static void getDBContents(String tableName, OnTupleGetListener onTupleGetListener){
		String csvFile = "/Users/pranavvij/Desktop/data/" + tableName.toLowerCase() + ".dat";
        BufferedReader br = null;
        String tuple = "";
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((tuple = br.readLine()) != null) {
            	if(!tuple.isEmpty()) onTupleGetListener.onTupleReceived(tuple, tableName);
            }
        } catch (FileNotFoundException e) {
        	e.printStackTrace();
      		System.out.println("Error 1 " + tableName);
       } catch (IOException e) {
       	e.printStackTrace();
     		System.out.println("Error 1 " + tableName);
      } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
	}
}