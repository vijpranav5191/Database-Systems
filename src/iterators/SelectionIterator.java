package iterators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import utils.EvaluateUtils;

public class SelectionIterator implements DefaultIterator {
	DefaultIterator iterator;
	private Expression whereExp;
	List<PrimitiveValue> nextResult;
	List<String> columns;
	Map<String, Integer> columnMapper;
	
	public SelectionIterator(DefaultIterator iterator, Expression whereExp){
		this.iterator = iterator;
		this.columns = this.iterator.getColumns();
		this.whereExp = whereExp;
		createMapperColumn();
		this.nextResult = getNextIter();
	}
	
	private void createMapperColumn() {
		this.columnMapper = new HashMap<String, Integer>();
		int index = 0;
		for(String col: this.columns) {
			this.columnMapper.put(col, index);
			index+=1;
		}
	}

	public DefaultIterator getIterator() {
		return iterator;
	}

	@Override
	public boolean hasNext() {
		if(this.nextResult != null) {
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

	@Override
	public void reset() {
		this.iterator.reset();
		this.nextResult = getNextIter();
	}

	@Override
	public List<String> getColumns() {
		return this.columns;
	}

	public List<PrimitiveValue> getNextIter() {
		List<PrimitiveValue> pos = this.iterator.next();
		try {
			while(pos != null && !EvaluateUtils.evaluate(pos, this.whereExp, this.columnMapper)) {
				pos = this.iterator.next();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return pos;
	}
}
