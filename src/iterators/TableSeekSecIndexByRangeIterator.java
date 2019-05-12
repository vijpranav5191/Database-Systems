package iterators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
import utils.Config;

public class TableSeekSecIndexByRangeIterator implements DefaultIterator {
	private List<String> columns;
	Table table;
	private String tuple;
	BufferedReader br;
	String indexColumn;
	int searchValue,endvalue;
	List<ColumnDefs> cdefs;
	Map<String, Integer> columnMap;
	List<Integer> seekList;
	private RandomAccessFile raf_1 = null;
	private Map<PrimitiveValue, List<Integer>> index;
	private ArrayList keylist;
	private Iterator<Integer> iterator;
	private int end;
	public TableSeekSecIndexByRangeIterator(Table table, int i, int e, String indexColumn, Map<PrimitiveValue, List<Integer>> index) {
		// TODO Auto-generated constructor stub
		this.columns = new ArrayList<String>();
		this.table = table;
		this.cdefs = SchemaStructure.schema.get(table.getName());
		for(int j = 0;j < this.cdefs.size(); j++) {
			if(this.table.getAlias() != null){
				this.columns.add(this.table.getAlias() + "." + cdefs.get(j).cdef.getColumnName());	
			} else {
				this.columns.add(table.getName() + "." + cdefs.get(j).cdef.getColumnName());
			}
		}
		this.end = e;
		this.searchValue = i;
		this.columnMap = createColumnMapper(this.cdefs);
		this.indexColumn = indexColumn;
		this.index = index;
		this.keylist = new ArrayList<>(this.index.keySet());
		try {
			if(this.raf_1 == null) {
				this.raf_1 = new RandomAccessFile(Config.databasePath + this.table.getName() + ".csv", "r");
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		this.iterator = this.index.get(this.keylist.get(this.searchValue)).iterator();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if(this.searchValue<=this.end) {
			return true;
		}
		else 
			return false;
	}

	@Override
	public List<PrimitiveValue> next() {
		// TODO Auto-generated method stub
		if(!this.iterator.hasNext())
		{
			this.searchValue++;
			this.iterator = this.index.get(this.keylist.get(this.searchValue)).iterator();
		}

		
		try {
			tuple = raf_1.readLine();
			raf_1.seek(this.iterator.next());
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<PrimitiveValue> map = new ArrayList<PrimitiveValue>();
		String[] row;
		if(tuple != null) {
			row = tuple.split("\\|");
		} else {
			return null;
		}
		for(int j = 0;j < row.length; j++) {
			ColumnDefs cdef = this.cdefs.get(j);
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
			map.add(pm);
		}
		return map;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public Map<String, Integer> createColumnMapper(List<ColumnDefs> cdefs) {
		Map<String, Integer> mapper = new HashMap<String, Integer>();
		int index = 0;
		for(ColumnDefs cdef: cdefs) {
			mapper.put(this.table.getName() + "." + cdef.cdef.getColumnName(), index);
			index+=1;
		}
		return mapper;
	}

}
