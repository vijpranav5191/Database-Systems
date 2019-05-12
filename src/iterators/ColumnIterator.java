package iterators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import utils.Config;

public class ColumnIterator implements RAIterator {
	private BufferedReader br;
	private String csvFile;
	private List<String> columns;
	private String tuple;
	String column;
	
	ColumnIterator(String column, String path){
		this.column = column;
		this.csvFile = path + column;
		this.columns = new ArrayList<>();
		this.columns.add(column);
		
		try {
			br = new BufferedReader(new FileReader(csvFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean hasNext() {
		try {
			if(br.ready()) {
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;	
		}
		return false;
	}

	@Override
	public String next() {
		try {
			tuple = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tuple.toString();
	}

	@Override
	public void reset() {
		try {
			br.close();
			br = new BufferedReader(new FileReader(this.csvFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<String> getColumns() {
		return this.columns;
	}
}
