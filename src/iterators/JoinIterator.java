package iterators;

import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.Join;
import utils.EvaluateUtils;

public class JoinIterator implements DefaultIterator{
	DefaultIterator leftIterator;
	DefaultIterator rightIterator;
	Map<String, PrimitiveValue> leftTuple;
	Join join;
	
	public JoinIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join) {
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.leftTuple = leftIterator.next();
		this.join = join;
	}
	
	@Override
	public boolean hasNext() {
		if(!this.leftIterator.hasNext() && !this.rightIterator.hasNext()) {
			return false;
		}
		return true;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Expression exp = this.join.getOnExpression();
		Map<String, PrimitiveValue> temp = this.getNextIter();
		if(exp != null) {
			try {
				while(temp != null && !EvaluateUtils.evaluate(temp, exp)) {
					//System.out.println(EvaluateUtils.evaluate(temp, exp));
					//System.out.println(temp);
					temp = this.getNextIter();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return temp;
	}

	@Override
	public void reset() {
		this.leftIterator.reset();
		this.rightIterator.reset();
		this.leftTuple = leftIterator.next();
	}
	
	
	public Map<String, PrimitiveValue> getNextIter(){
		Map<String, PrimitiveValue> temp = new HashMap<String, PrimitiveValue>();
		if(!rightIterator.hasNext()) {
			this.leftTuple = this.leftIterator.next();
			this.rightIterator.reset();
		}
		if(this.leftTuple == null) {
			return null;
		}
		Map<String, PrimitiveValue> rightTuple = this.rightIterator.next();
		for(String key: rightTuple.keySet()) {
			temp.put(key, rightTuple.get(key));
		}
		for(String key: this.leftTuple.keySet()) {
			temp.put(key, this.leftTuple.get(key));
		}
		return temp;
	}
}
