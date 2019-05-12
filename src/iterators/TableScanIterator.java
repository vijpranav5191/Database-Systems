package iterators;


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
import utils.Config;


public class TableScanIterator implements DefaultIterator {
	public Table tab;
	private List<String> columns;
	List<ColumnDefs> cdefs;
	Map<String, Integer> columnMap;
	private List<ColumnIterator> columnIterator;
	String path;
	
	public TableScanIterator( Table tab, List<String> queryColumns, String path) {
		this.columns = queryColumns;
		this.tab = tab;
		this.path = path;
		this.cdefs = SchemaStructure.schema.get(this.tab.getName());
		this.columnMap = createColumnMapper(this.cdefs);
		this.columnIterator = new ArrayList<>();
		
		for(String col: this.columns) {
			ColumnIterator colIter = new ColumnIterator(col, this.path);
			columnIterator.add(colIter);
		}
	}
	
	public TableScanIterator( Table tab, List<String> queryColumns) {
		this.columns = queryColumns;
		this.tab = tab;
		this.path = Config.columnSeparator;
		this.cdefs = SchemaStructure.schema.get(this.tab.getName());
		this.columnMap = createColumnMapper(this.cdefs);
		this.columnIterator = new ArrayList<>();
		
		for(String col: this.columns) {
			ColumnIterator colIter = new ColumnIterator(col, this.path);
			columnIterator.add(colIter);
		}
	}
	
	@Override
	public boolean hasNext() {
		if(this.columnIterator.isEmpty()) {
			return false;
		}
		return this.columnIterator.get(0).hasNext();
	}
	
	public List<PrimitiveValue> next() {
		if(this.hasNext()) {
			List<PrimitiveValue> mapList = new ArrayList<PrimitiveValue>();
			for(int i = 0;i < this.columns.size();i++){
				String elem = this.columns.get(i);
				if( this.columnMap.containsKey(elem) ) {
					PrimitiveValue pm;
					int index = this.columnMap.get(elem);
					ColumnIterator colIter = this.columnIterator.get(i);
					
					ColumnDefs cdef = this.cdefs.get(index);
					String value = colIter.next();
					
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
		if(this.columnIterator != null) {
			for(ColumnIterator colIter: columnIterator) {
				colIter.reset();
			}
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
			mapper.put(this.tab.getName() + "." + cdef.cdef.getColumnName(), index);
			index+=1;
		}
		return mapper;
	}
}