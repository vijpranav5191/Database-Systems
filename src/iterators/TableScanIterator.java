package iterators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
//import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import objects.ColumnDefs;
import objects.SchemaStructure;
import queryexec.CreateWrapper;
import utils.Config;
import utils.Utils;


public class TableScanIterator implements DefaultIterator {
	private String csvFile;
	private String tableName;
	private BufferedReader br;
	private String tuple;
	public Table tab;
	private List<PrimitiveValue> map;
	private List<String> columns;
	List<ColumnDefs> cdefs;
	Map<String, Integer> columnMap;
	private List<PrimitiveValue> mapList;
	
	
	public TableScanIterator( Table tab, List<String> queryColumns) {
		this.columns = queryColumns;
		this.tableName = tab.getName();
		this.tab = tab;
		this.cdefs = SchemaStructure.schema.get(tableName);
		this.columnMap = createColumnMapper(this.cdefs);
		
		this.csvFile = Config.databasePath + tableName + ".csv";;	
		try {
			br = new BufferedReader(new FileReader(csvFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Error 1 " + tableName);
		}
		tuple = "";
	}
	
	
	@Override
	public boolean hasNext() {
		try {
			if(br.ready()) {
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
		if(this.hasNext()) {
			mapList = new ArrayList<PrimitiveValue>();
			try {
				tuple = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			String row[] = tuple.split("\\|");
			for(String elem : this.columns){
				if( this.columnMap.containsKey(elem) ) {
					PrimitiveValue pm;
					int index = this.columnMap.get(elem);
					ColumnDefs cdef = this.cdefs.get(index);
					String value = row[index];
					
					switch (cdef.cdef.getColDataType().getDataType().toLowerCase()) {
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
					mapList.add(pm);
				}
			}
			return mapList;
		}
		return null;
	}

	@Override
	public void reset() {
		try {
			br.close();
			br = new BufferedReader(new FileReader(this.csvFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Error 1 " + tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<String> getColumns() {
		return  this.columns;
	}
	
	public Map<String, Integer> createColumnMapper(List<ColumnDefs> cdefs) {
		Map<String, Integer> mapper = new HashMap<String, Integer>();
		int index = 0;
		for(ColumnDefs cdef: cdefs) {
			mapper.put(tableName + "." + cdef.cdef.getColumnName(), index);
			index+=1;
		}
		return mapper;
	}
}