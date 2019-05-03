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

public class TableSeekIterator implements DefaultIterator{

	private List<String> columns;
	Table table;
	private String tuple;
	private Map<String, PrimitiveValue> nextResult;
	BufferedReader br;
	String indexColumn;
	PrimitiveValue searchValue;
	
	public TableSeekIterator(BufferedReader br, Table table, PrimitiveValue searchValue, String indexColumn){
		this.columns = new ArrayList<String>();
		this.br = br;
		this.table = table;
		this.searchValue = searchValue;
		this.indexColumn = indexColumn;
		this.nextResult = getNextIter();
	}
	
	@Override
	public boolean hasNext() {
		if(this.nextResult != null && this.nextResult.get(indexColumn).equals(searchValue)) {
			return true;
		}
		return false;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> temp = this.nextResult;
		this.nextResult = getNextIter();
		return temp;
	}


	public Map<String, PrimitiveValue> getNextIter(){
		try {
			tuple = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Map<String, PrimitiveValue> map = new HashMap<String, PrimitiveValue>();
		String[] row;
		if(tuple != null) {
			row = tuple.split("\\|");
		} else {
			return null;
		}
		List<ColumnDefs> cdefs = SchemaStructure.schema.get(this.table.getName());
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
			if(this.table.getAlias() != null) {
				map.put( this.table.getAlias() + "." + cdef.cdef.getColumnName(), pm);	
			} else {
				map.put( this.table.getName() + "." + cdef.cdef.getColumnName(), pm);
			}
		}
		return map;
	}
	
	@Override
	public void reset() {
		new Exception("Please dont Reset!!!!!");
	}

	@Override
	public List<String> getColumns() {
		if(this.columns.size() == 0) {
			List<ColumnDefs> cdefs = SchemaStructure.schema.get(this.table.getName());
			for(int j = 0;j < cdefs.size(); j++) {
				if(this.table.getAlias() != null){
					this.columns.add(this.table.getAlias() + "." + cdefs.get(j).cdef.getColumnName());	
				}else {
					this.columns.add(this.table.getName() + "." + cdefs.get(j).cdef.getColumnName());
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
