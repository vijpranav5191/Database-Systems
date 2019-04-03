package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;

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
	
	public SortMergeIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join) throws Exception {
		this.join = join;
		this.rightTuple = rightIterator.next(); 
		
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
		
//		System.out.println(leftExpression);	
//		while(leftIterator.hasNext()) {
//			System.out.println(leftIterator.next());
//		}
//		
//		System.out.println(rightExpression);	
//		while(rightIterator.hasNext()) {
//			System.out.println(rightIterator.next());
//		}
		
		rightIterator.reset();
		//leftIterator.reset();
			
		
		// Left Iteration after sorting
		OrderByElement leftOrderByElement = new OrderByElement();
		Column colLeft = new Column();
		
		Table  tableLeft = new Table();
		tableLeft.setName(this.leftExpression.split("\\.")[0]);
		colLeft.setColumnName(this.leftExpression.split("\\.")[1]);
		colLeft.setTable(tableLeft);
		leftOrderByElement.setExpression(colLeft);
		
		List<OrderByElement> listLeft = new ArrayList<OrderByElement>();
		listLeft.add(leftOrderByElement);	
		this.leftIterator = new OrderByIterator(listLeft, leftIterator);
			
		// Right Iteration after sorting
		OrderByElement rightOrderByElement = new OrderByElement();
		Column colRight = new Column();

		Table  tableRight = new Table();
		tableRight.setName(this.rightExpression.split("\\.")[0]);
		colRight.setColumnName(this.rightExpression.split("\\.")[1]);
		colRight.setTable(tableRight);
		rightOrderByElement.setExpression(colRight);
		
		List<OrderByElement> listRight = new ArrayList<OrderByElement>();
		listRight.add(rightOrderByElement);
		this.rightIterator = new OrderByIterator(listRight, rightIterator);
		
//		System.out.println(leftExpression);	
//		while(this.leftIterator.hasNext()) {
//			System.out.println(this.leftIterator.next());
//		}
//		
//		System.out.println(rightExpression);	
//		while(this.rightIterator.hasNext()) {
//			System.out.println(this.rightIterator.next());
//		}
//		
//		this.rightIterator.reset();
//		this.leftIterator.reset();
		
		
		this.rightTuple = this.rightIterator.next();
		this.leftTuple = this.leftIterator.next();
		setNextLeftIterator();
		
		this.columns = new ArrayList<String>();
		
	}
	
	@Override
	public boolean hasNext() {
		if(index == 0 && this.rightTuple == null) {
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
		Map<String, PrimitiveValue> leftTuple = this.leftTupleList.get(this.index);
		Map<String, PrimitiveValue> result = pushTogetherMap(leftTuple, this.rightTuple);
		this.index++;
		if(index >= this.leftTupleList.size()) {
			Map<String, PrimitiveValue> temp = this.rightIterator.next();
			this.index = 0;
			if(temp != null && !temp.get(this.rightExpression).equals(this.rightTuple.get(this.rightExpression))) {
				setNextLeftIterator();
			}
			this.rightTuple = temp;
		}
		return result;
	}
	
	public void setNextLeftIterator() {
		this.leftTupleList = new ArrayList<Map<String, PrimitiveValue>>();
		this.leftTupleList.add(this.leftTuple);
		Map<String, PrimitiveValue> next = this.leftIterator.next();
		while(next != null && this.leftTuple.get(leftExpression).equals(next.get(leftExpression))) {
			this.leftTuple = next;
			next = this.leftIterator.next();
			this.leftTupleList.add(this.leftTuple);
		}
		this.leftTuple = next;
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return this.leftIterator;
	}
}
