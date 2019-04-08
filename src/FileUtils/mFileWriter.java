package FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class mFileWriter {
	File file;
	BufferedWriter writer;
	List<String> columns;

	public mFileWriter(String path, String id, List<String> columns) throws IOException { 
		file = new File(path + id + ".dat");
		this.columns = columns;
		writer = new BufferedWriter(new FileWriter(file, true));   
	}


	public void writeNext(Map<String, PrimitiveValue> map) throws IOException {
		String writeInFile = "";
		for (String col : this.columns){
			writeInFile += String.valueOf(map.get(col)) + "|"; 
		}
		String writeInFile1 = writeInFile.substring(0, writeInFile.length() - 1);
		writer.write(writeInFile1);
		writer.newLine();
	}
	
	public void close() throws IOException {
		this.writer.close();
	}
}
