package iterators;

import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.Limit;

public class LimitIterator implements DefaultIterator{
	DefaultIterator iterator;
	Limit limit;
	int index = 0;
	
	public LimitIterator(DefaultIterator iterator, Limit limit) {
		this.iterator = iterator;
		this.limit = limit;
	}

	
	@Override
	public boolean hasNext() {
		if(this.index < this.limit.getRowCount()) {
			return true;
		}
		return false;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> temp = null;
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
		return this.iterator.getColumns();
	}

}
