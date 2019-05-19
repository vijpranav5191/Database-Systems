package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import objects.ColumnDefs;
import objects.SchemaStructure;

public class InsertIterator implements DefaultIterator {
	
	DefaultIterator oldIterator;
	Iterator<List<PrimitiveValue>> updateIterator;
	List<List<PrimitiveValue>> insertList;
	Table table;
	private List<String> columns;
	Map<String, Integer> columnMapper;
	
	public InsertIterator(DefaultIterator oldIterator,
			List<List<PrimitiveValue>> insertList, List<String> queryColumns, Table table) {
		this.columns = queryColumns;
		this.oldIterator = oldIterator;
		this.insertList = insertList;
		this.table = table;
		this.updateIterator = insertList.iterator();
		this.columnMapper = createColumnMapper(SchemaStructure.schema.get(table.getName()));
	}
	@Override
	public boolean hasNext() {
		if(!this.oldIterator.hasNext() && !this.updateIterator.hasNext()) {
			return false;
		}
		else {
			return true;
		}
	}

	@Override
	public List<PrimitiveValue> next() {
		if (this.oldIterator.hasNext()) {
			return this.oldIterator.next();
		}
		if(this.updateIterator.hasNext()) {
			List<PrimitiveValue> map = this.updateIterator.next();
			List<PrimitiveValue> tuple = new ArrayList<>();
			for(String col: this.columns) {
				tuple.add(map.get(this.columnMapper.get(col)));
			}
			return tuple;
		}
		
		return null;
	}

	@Override
	public void reset() {
		this.oldIterator.reset();
		this.updateIterator = this.insertList.iterator();

	}

	@Override
	public List<String> getColumns() {
		return this.columns;
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