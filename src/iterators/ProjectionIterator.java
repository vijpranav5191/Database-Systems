package iterators;

import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import utils.EvaluateUtils;

public class ProjectionIterator implements DefaultIterator {
	TableScanIterator iterator;
	private Expression whereExp;
	Map<String, PrimitiveValue> nextResult;
	
	public ProjectionIterator(TableScanIterator iterator, Expression whereExp){
		this.iterator = iterator;
		this.whereExp = whereExp;
		
	}
	
	@Override
	public boolean hasNext() {
		return this.iterator.hasNext();
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> temp = this.nextResult;
		
		Map<String, PrimitiveValue> pos = this.iterator.next();
		while(this.iterator.hasNext() && !evaluate(pos)) {
			pos = this.iterator.next();
		}
		if(this.iterator.hasNext() && temp == null) { // for first
			temp = pos;
			pos = this.iterator.next();
			while(this.iterator.hasNext() && !evaluate(pos)) {
				pos = this.iterator.next();
			}
			this.nextResult = pos;
		} else {
			this.nextResult = pos; 
		}
		return temp;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.iterator.reset();
		this.nextResult = null;
	}

	public Boolean evaluate(Map<String, PrimitiveValue> map) {
		if(this.whereExp != null) {
			try {
				return EvaluateUtils.evaluate(map, this.whereExp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		} else {
			System.out.println(map.toString());
		}
		return false;
	}
}
