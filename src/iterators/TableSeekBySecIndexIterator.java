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

public class TableSeekBySecIndexIterator implements DefaultIterator {

	private List<String> columns;
	Table table;
	private String tuple;
	private List<PrimitiveValue> nextResult;
	BufferedReader br;
	String indexColumn;
	String searchValue;
	List<ColumnDefs> cdefs;
	Map<String, Integer> columnMap;
	List<Integer> seekList;
	private RandomAccessFile raf_1 = null;
	private Iterator<Integer> iter;
	public TableSeekBySecIndexIterator(List<Integer> slist , Table table, String string, String indexColumn){
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
		this.columnMap = createColumnMapper(this.cdefs);
		this.searchValue = string;
		this.indexColumn = indexColumn;
		this.nextResult = getNextIter();
		try {
			if(raf_1 == null) {
				raf_1 = new RandomAccessFile(Config.databasePath + this.table.getName() + ".csv", "r");
			}
			raf_1.seek(slist.get(0));
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.seekList = slist;
		this.iter = this.seekList.iterator();

	}
	

	@Override
	public boolean hasNext() {	
		return this.iter.hasNext();
	}

	@Override
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> temp = this.nextResult;
		this.nextResult = getNextIter();
		return temp;
	}


	public List<PrimitiveValue> getNextIter(){
		try {
			tuple = raf_1.readLine();
			raf_1.seek(this.iter.next());
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
		new Exception("Please dont Reset!!!!!");
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
	
	@Override
	public List<String> getColumns() {
		return this.columns;
	}
}
