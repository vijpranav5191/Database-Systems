package iterators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import objects.ColumnDefs;
import objects.SchemaStructure;

public class TableScanIterator implements DefaultIterator{
	String csvFile;
	String tableName;
	BufferedReader br;
	String tuple;
	public TableScanIterator(Table tab) {
		// TODO Auto-generated constructor stub
		this.tableName = tab.getName();
		this.csvFile = "/Users/pranavvij/Desktop/data/" + tableName.toLowerCase() + ".dat";
		try {
			br = new BufferedReader(new FileReader(csvFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Error 1 " + tableName);
		}
		tuple = "";
	}
	public boolean hasNext() {
		try {
			if((this.tuple = this.br.readLine()) !=null) {
				return true;
			}
			else return false;
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error 2 " + tableName);
			return false;
			
		}
	}
	public List<PrimitiveValue> next() {

		String[] row = tuple.split("\\|");
		List<ColumnDefs> cdefs = SchemaStructure.schema.get(tableName);
		List<PrimitiveValue> rowlist = new ArrayList<>();
		for(int j = 0;j < row.length; j++) {
			ColumnDefs cdef = cdefs.get(j);
			String value = row[j];
			PrimitiveValue pm;
			switch (cdef.cdef.getColDataType().getDataType()) {
			case "int":
				pm = new LongValue(value);
				break;
			case "string":
				pm = new StringValue(value);
				break;
			case "varchar":
				pm = new StringValue(value);
				break;	
			case "char":
				pm = new StringValue(value);
				break;
			case "decimal":
				pm = new DoubleValue(value);
				break;
			case "date":
				pm = new DateValue(value);
				break;
			default:
				pm = new StringValue(value);
				break;
			}
			rowlist.add(pm);
			//System.out.println(tableName + "." + cdef.cdef.getColumnName() + ":" + pm);
		}
		return rowlist;
	}
}
