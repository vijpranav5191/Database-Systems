package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.select.Join;

public class SortMergeIterator implements DefaultIterator{
	DefaultIterator leftIterator;
	DefaultIterator rightIterator;

	
	Join join;
	List<String> columns;
	String leftExpression;
	String rightExpression;
	Map<String, PrimitiveValue> rightTuple;
	Map<String, PrimitiveValue> leftTuple;
	ArrayList<Map<String, PrimitiveValue>> leftTupleList;
	int index = 0;
	
	public SortMergeIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join) {
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.join = join;
		this.rightTuple = this.rightIterator.next();
		this.leftTuple = this.leftIterator.next();
		setNextLeftIterator();
		this.columns = new ArrayList<String>();
		if(this.join.getOnExpression() instanceof EqualsTo) {
			EqualsTo exp = (EqualsTo) this.join.getOnExpression();
			this.leftExpression = exp.getLeftExpression().toString(); // need to check this approach
			this.rightExpression = exp.getRightExpression().toString();
			if(!this.rightTuple.containsKey(this.rightExpression)) {
				String temp = this.leftExpression;
				this.leftExpression = this.rightExpression;
				this.rightExpression = temp;
			}
		}
	}
	
	@Override
	public boolean hasNext() {
		if(!this.rightIterator.hasNext()) {
			return false;
		}
		return true;
	}
	
	@Override
	public Map<String, PrimitiveValue> next() {
		return getNextIter();
	}

	@Override
	public void reset() {
		this.leftIterator.reset();
		this.rightIterator.reset();
	}
	
	public Map<String, PrimitiveValue> pushTogetherMap(Map<String, PrimitiveValue> leftTuple,Map<String, PrimitiveValue> rightTuple){
		Map<String, PrimitiveValue> temp = new HashMap<String, PrimitiveValue>();
		
		for(String key: rightTuple.keySet()) {
			temp.put(key, rightTuple.get(key));
		}
		for(String key: leftTuple.keySet()) {
			temp.put(key, leftTuple.get(key));
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
	
	public Map<String, PrimitiveValue> getNextIter(){
		Map<String, PrimitiveValue> leftTuple = this.leftTupleList.get(index);
		index++;
		if(index >= this.leftTupleList.size()) {
			Map<String, PrimitiveValue> temp = this.rightIterator.next();
			index = 0;
			if(temp.get(rightExpression).equals(this.rightTuple.get(rightExpression))) {
				this.rightTuple = temp;
			} else {
				setNextLeftIterator();
			}
		}
		return pushTogetherMap(leftTuple, this.rightTuple);
	}
	
	public void setNextLeftIterator() {
		this.leftTupleList = new ArrayList<Map<String, PrimitiveValue>>();
		Map<String, PrimitiveValue> next = this.leftIterator.next();
		this.leftTupleList.add(this.leftTuple);
		while(this.leftTuple.get(leftExpression).equals(next.get(leftExpression))) {
			this.leftTuple = next;
			next = this.leftIterator.next();
			this.leftTupleList.add(this.leftTuple);
		}
		this.leftTuple = next;
	}
}
