package iterators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import objects.ColumnDefs;
import objects.SchemaStructure;
import utils.Config;

public class FileReaderIterator implements RAIterator {
	Table table;
	String csvFile;
	private String tuple;
	private BufferedReader br;
	private List<String> columns;
		
	public FileReaderIterator(Table table){
		this.columns = new ArrayList<String>();
		this.table = table;
		this.csvFile = Config.databasePath + table.getName() + ".csv";
		try {
			br = new BufferedReader(new FileReader(csvFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Error 1 " + table.getName());
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
			System.out.println("Error 2 " + this.table.getName());
		}
		return false;
	}

	@Override
	public String next() {
		if(this.hasNext()) {
			try {
				tuple = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return tuple;
	}

	@Override
	public void reset() {
		try {
			br.close();
			br = new BufferedReader(new FileReader(this.csvFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Error 1 " + table.getName());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<String> getColumns() {
		if(this.columns.size() == 0) {
			List<ColumnDefs> cdefs = SchemaStructure.schema.get(table.getName());
			for(int j = 0;j < cdefs.size(); j++) {
				if(this.table.getAlias() != null){
					this.columns.add(this.table.getAlias() + "." + cdefs.get(j).cdef.getColumnName());	
				}else {
					this.columns.add(table.getName() + "." + cdefs.get(j).cdef.getColumnName());
				}
			}
		}
		return this.columns;
	}
}
