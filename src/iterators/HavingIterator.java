package iterators;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectItem;
import utils.EvaluateUtils;

public class HavingIterator implements DefaultIterator{
	DefaultIterator iterator;
	Expression havingClause;
	Map<String, PrimitiveValue> nextResult;
	List<SelectItem> selectItems;
	public HavingIterator(DefaultIterator iterator, Expression havingclause, List<SelectItem> selectItems) {
		// TODO Auto-generated constructor stub
		this.iterator = iterator;
		this.havingClause = havingclause;
		this.nextResult = getNextIter();
		this.selectItems = selectItems;
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if(this.nextResult != null) {
			return true;
		}
		return false;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		if(this.hasNext()) {
			Map<String, PrimitiveValue> temp = this.nextResult;
			this.nextResult = getNextIter();
			return temp;
		}
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
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
//		boolean hasAgg= false;
//		for(SelectItem sel : this.selectItems) {
//			if(sel instanceof Function) hasAgg = true;
//		}
//		if(this.havingClause instanceof Function && !hasAgg) {
//			Function func = (Function) this.havingClause;
//			this.iterator.reset();
//			DefaultIterator iter = new SimpleAggregateIterator(this.iterator, func);
//			pos.putAll(iter.next());	
//		}
		try {
			while(pos != null && !EvaluateUtils.evaluate(pos, this.havingClause)) {
				pos = this.iterator.next();
			}
		} catch (Exception e1) {
			pos = null;
		} 
		return pos;
	}
	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return this.iterator;
	}
}
