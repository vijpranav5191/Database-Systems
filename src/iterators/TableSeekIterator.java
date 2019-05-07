package iterators;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class TableSeekIterator implements DefaultIterator {

	private List<String> columns;
	Table table;
	private String tuple;
	private List<PrimitiveValue> nextResult;
	BufferedReader br;
	String indexColumn;
	PrimitiveValue searchValue;
	List<ColumnDefs> cdefs;
	Map<String, Integer> columnMap;
	Map<String, Integer> queryMap;
	
	public TableSeekIterator(BufferedReader br, Table table, PrimitiveValue searchValue, String indexColumn, List<String> queryColumns){
		this.columns = queryColumns;
		this.br = br;
		this.table = table;
		this.queryMap = new HashMap<String, Integer>();
		for(int i =0;i < this.columns.size();i++) {
			this.queryMap.put(this.columns.get(i), i);
		}
		this.cdefs = SchemaStructure.schema.get(table.getName());
		this.columnMap = createColumnMapper(this.cdefs);
		this.searchValue = searchValue;
		this.indexColumn = indexColumn;
		this.nextResult = getNextIter();
	}
	
	@Override
	public boolean hasNext() {
		if(this.nextResult != null && this.nextResult
				.get(this.queryMap.get(indexColumn))
				.equals(searchValue)) {
			return true;
		}
		return false;
	}

	@Override
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> temp = this.nextResult;
		this.nextResult = getNextIter();
		return temp;
	}


	public List<PrimitiveValue> getNextIter(){
		try {
			tuple = br.readLine();
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
				map.add(pm);
			}
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
