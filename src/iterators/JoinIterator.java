package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
	List<String> columns;
	
	public JoinIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join) {
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.leftTuple = leftIterator.next();
		this.join = join;
		this.columns = new ArrayList<String>();
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
		Map<String, PrimitiveValue> temp = this.getNextIter();
		Expression exp = this.join.getOnExpression();
		if(exp != null) {
			try {
				while(temp != null && !EvaluateUtils.evaluate(temp, exp)) {
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
			if(this.leftIterator.hasNext()) {
				this.leftTuple = this.leftIterator.next();
				this.rightIterator.reset();
			}
		}
		Map<String, PrimitiveValue> rightTuple = this.rightIterator.next();
		if(this.leftTuple == null || rightTuple == null) {
			return null;
		}
		for(String key: rightTuple.keySet()) {
			temp.put(key, rightTuple.get(key));
		}
		for(String key: this.leftTuple.keySet()) {
			temp.put(key, this.leftTuple.get(key));
		}
		return temp;
	}

	@Override
	public List<String> getColumns() {
		if(this.columns.size() == 0) {
			this.columns.addAll(this.leftIterator.getColumns());
			this.columns.addAll(this.rightIterator.getColumns());
		}
		return this.columns;
	}
}
