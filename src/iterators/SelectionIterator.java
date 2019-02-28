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
	}
	
	@Override
	public boolean hasNext() {
		return this.iterator.hasNext();
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> pos = this.iterator.next();
		try {
			while(this.iterator.hasNext() && pos!=null && !EvaluateUtils.evaluate(pos, this.whereExp)) {
				pos = this.iterator.next();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
//		if(this.iterator.hasNext() && temp == null) { // for first
//			temp = pos;
//			pos = this.iterator.next();
//			try {
//				while(this.iterator.hasNext() && !EvaluateUtils.evaluate(pos, this.whereExp)) {
//					pos = this.iterator.next();
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//			this.nextResult = pos;
//		} else {
//			this.nextResult = pos; 
//		}
		return pos;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.iterator.reset();
		this.nextResult = null;
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		return this.iterator.getColumns();
	}
}
