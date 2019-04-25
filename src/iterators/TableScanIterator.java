package iterators;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
//import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import objects.ColumnDefs;
import objects.SchemaStructure;
import queryexec.CreateWrapper;


public class TableScanIterator implements DefaultIterator {
	private Boolean DEBUG = false;
	private String csvFile;
	private String tableName;
	private BufferedReader br;
	private String tuple;
	private Table tab;
	private Map<String, PrimitiveValue> map;
	private List<String> columns;
	private Boolean isOrderBy;
	
	public TableScanIterator(Table tab ) {
		this.columns = new ArrayList<String>();
		this.tableName = tab.getName();
		this.tab = tab;
		if(DEBUG) {

//			this.csvFile = "C:\\Users\\Amit\\Desktop\\Sanity_Check_Examples\\data\\" + tableName.toLowerCase() + ".dat";
			this.csvFile = "C:\\Users\\Amit\\Desktop\\Sanity_Check_Examples\\data\\50Data\\" + tableName.toLowerCase() + ".csv";

//			this.csvFile = "C:\\Users\\ayush\\Documents\\Sanity_Check_Examples\\data\\" + tableName.toLowerCase() + ".tbl";			
//			this.csvFile = "C:\\Users\\Amit\\Desktop\\Sanity_Check_Examples\\data\\50Data\\" + tableName.toLowerCase() + ".csv";

//			this.csvFile = "C:\\Users\\ayush\\Documents\\Sanity_Check_Examples\\data\\" + tableName.toLowerCase() + ".dat";			
//			this.csvFile = "/Users/pranavvij/Desktop/data/checkpoint2/" + tableName.toLowerCase() + ".csv";
		} else {
			this.csvFile = "data/" + tableName + ".csv";		
		}
			try {
				br = new BufferedReader(new FileReader(csvFile));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				System.out.println("Error 1 " + tableName);
			}

		tuple = "";
		List<ColumnDefs> cdefs = SchemaStructure.schema.get(tableName);
		if(cdefs==null) {
			getCreateFromDisk();
		}
	}
	
	public TableScanIterator(Table tab, Boolean isOrderBy, File fileName )
	{
		this.columns = new ArrayList<String>();
		this.tableName = tab.getName();
		this.tab = tab;
		this.isOrderBy = isOrderBy;
		if(DEBUG) {
			if(isOrderBy)
				this.csvFile = String.valueOf(fileName);		
		} else {
			if(isOrderBy == true)
				this.csvFile = String.valueOf(fileName);
			
		}
		try {
			br = new BufferedReader(new FileReader(csvFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Error 1 " + tableName);
		}
		tuple = "";
	}
	public void getCreateFromDisk() {
		try {
			FileInputStream readOb = new FileInputStream("createdir/"+this.tableName.toLowerCase()+".txt");
			CCJSqlParser parser2 = new CCJSqlParser(readOb);
			Statement query = parser2.Statement();
			CreateWrapper cw = new CreateWrapper();
			cw.createHandler(query);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	
	@Override
	public Map<String, PrimitiveValue> next() {
		if(this.hasNext()) {
			try {
				tuple = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			map = new HashMap<String, PrimitiveValue>();
			String[] row = tuple.split("\\|");
			List<ColumnDefs> cdefs = SchemaStructure.schema.get(tableName);
			if(cdefs==null) {
				try {
					FileInputStream readOb = new FileInputStream("createdir/"+this.tableName.toLowerCase()+".txt");
					CCJSqlParser parser = new CCJSqlParser(readOb);
					Statement query = parser.Statement();
					CreateWrapper cw = new CreateWrapper();
					cw.createHandler(query);
					cdefs = SchemaStructure.schema.get(this.tableName);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for(int j = 0;j < row.length; j++) {
				ColumnDefs cdef = cdefs.get(j);
				String value = row[j];
				PrimitiveValue pm;
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
				if(this.tab.getAlias() != null) {
					this.map.put( this.tab.getAlias() + "." + cdef.cdef.getColumnName(), pm);	
				} else {
					this.map.put( this.tableName + "." + cdef.cdef.getColumnName(), pm);
				}
			}
			return map;
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
		if(this.columns.size() == 0) {
			List<ColumnDefs> cdefs = SchemaStructure.schema.get(tableName);
			for(int j = 0;j < cdefs.size(); j++) {
				if(this.tab.getAlias() != null){
					this.columns.add(this.tab.getAlias() + "." + cdefs.get(j).cdef.getColumnName());	
				}else {
					this.columns.add(tableName + "." + cdefs.get(j).cdef.getColumnName());
				}
			}
		}
		return this.columns;
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return null;
	}
	
}