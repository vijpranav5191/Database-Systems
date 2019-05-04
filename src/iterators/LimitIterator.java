package iterators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.Limit;

public class LimitIterator implements DefaultIterator{
	DefaultIterator iterator;
	Limit limit;
	int index = 0;
	List<String> columns;
	Map<String, Integer> columnMapper;
	
	public LimitIterator(DefaultIterator iterator, Limit limit) {
		this.iterator = iterator;
		this.limit = limit;
		this.columns = this.iterator.getColumns();
		createMapperColumn();
	}

	private void createMapperColumn() {
		this.columnMapper = new HashMap<String, Integer>();
		int index = 0;
		for(String col: this.columns) {
			this.columnMapper.put(col, index);
			index+=1;
		}
	}
	
	@Override
	public boolean hasNext() {
		if(this.iterator.hasNext() && this.index < this.limit.getRowCount()) {
			return true;
		}
		return false;
	}

	@Override
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> temp = null;
		if(index < this.limit.getRowCount()) {
			temp =  this.iterator.next();
			this.index++;
		}
		return temp;
	}

	@Override
	public void reset() {
		index = 0;
		this.iterator.reset();
	}

	@Override
	public List<String> getColumns() {
		return this.columns;
	}
}
