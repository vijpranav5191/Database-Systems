package iterators;

import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import utils.EvaluateUtils;

public class SelectionIterator implements DefaultIterator {
	DefaultIterator iterator;
	private Expression whereExp;
	Map<String, PrimitiveValue> nextResult;
	
	public SelectionIterator(DefaultIterator iterator, Expression whereExp){
		this.iterator = iterator;
		this.whereExp = whereExp;
		this.nextResult = getNextIter();
	}
	
	
	
	public DefaultIterator getIterator() {
		return iterator;
	}

	public void setIterator(DefaultIterator iterator) {
		this.iterator = iterator;
	}

	public Expression getWhereExp() {
		return whereExp;
	}

	public void setWhereExp(Expression whereExp) {
		this.whereExp = whereExp;
	}

	public Map<String, PrimitiveValue> getNextResult() {
		return nextResult;
	}

	public void setNextResult(Map<String, PrimitiveValue> nextResult) {
		this.nextResult = nextResult;
	}

	@Override
	public boolean hasNext() {
		if(this.nextResult != null) {
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

	@Override
	public void reset() {
		this.iterator.reset();
		this.nextResult = getNextIter();
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
			return this.iterator.getColumns();
	}

	public Map<String, PrimitiveValue> getNextIter() {
		Map<String, PrimitiveValue> pos = this.iterator.next();
		try {
			while(pos != null && !EvaluateUtils.evaluate(pos, this.whereExp)) {
				pos = this.iterator.next();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return pos;
	}
	public DefaultIterator getChildIter() {
		return this.iterator;
	}
}
