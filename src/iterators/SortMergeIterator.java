package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import utils.EvaluateUtils;

public class SortMergeIterator implements DefaultIterator{
	DefaultIterator leftIterator;
	DefaultIterator rightIterator;
	Join join;
	List<String> columns;
	String leftExpression;
	String rightExpression;
	EqualsTo equalTo;
	GreaterThan gtt;
	Map<String, PrimitiveValue> nextResult;
	
	Map<String, PrimitiveValue> rightTuple;
	Map<String, PrimitiveValue> leftTuple;
	ArrayList<Map<String, PrimitiveValue>> rightTupleList;
	int index = 0;
	
	public SortMergeIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join) throws Exception {
		this.join = join;
		this.columns = new ArrayList<String>();
		this.rightTupleList = new ArrayList<Map<String, PrimitiveValue>>();
		this.equalTo = new EqualsTo(); 
		this.gtt = new GreaterThan();
		if(this.join.getOnExpression() instanceof EqualsTo) {
			EqualsTo exp = (EqualsTo) this.join.getOnExpression();
			this.leftExpression = exp.getLeftExpression().toString(); // need to check this approach
			this.rightExpression = exp.getRightExpression().toString();
			if(!isContainingColumns(this.leftExpression, leftIterator.getColumns())) {
				String temp = this.leftExpression;
				this.leftExpression = this.rightExpression;
				this.rightExpression = temp;
			}
		}
		
		// Left Iteration after sorting
		OrderByElement leftOrderByElement = new OrderByElement();
		Column colLeft = new Column();
		
		Table  tableLeft = new Table();
		tableLeft.setName(this.leftExpression.split("\\.")[0]);
		colLeft.setColumnName(this.leftExpression.split("\\.")[1]);
		colLeft.setTable(tableLeft);
		leftOrderByElement.setExpression(colLeft);
		leftOrderByElement.setAsc(true);
		
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
		rightOrderByElement.setAsc(true);
		
		this.equalTo.setLeftExpression(colLeft);
		this.equalTo.setRightExpression(colRight);
		
		this.gtt.setLeftExpression(colLeft);
		this.gtt.setRightExpression(colRight);
		
		
		List<OrderByElement> listRight = new ArrayList<OrderByElement>();
		listRight.add(rightOrderByElement);
		this.rightIterator = new OrderByIterator(listRight, rightIterator);
		
		this.rightTuple = this.rightIterator.next();
		this.leftTuple = this.leftIterator.next();
		setNextLeftIterator();
		this.nextResult = this.getNextIter();
	}
	
	private boolean isContainingColumns(String leftExpression, List<String> columns) {
		for(String column: columns) {
			if(column.equals(leftExpression)) {
				return true;
			}
			String[] columnSplt = column.split("\\.");
			for(String split: columnSplt) {
				if(split.equals(leftExpression)) {
					return true;
				}
			}
		}
		return false;
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

	public Map<String, PrimitiveValue> getNextIter(){
		Map<String, PrimitiveValue> rightTuple = this.rightTupleList.get(this.index);
		Map<String, PrimitiveValue> result = pushTogetherMap(rightTuple, this.leftTuple);
		this.index++;
		
		if(index >= this.rightTupleList.size()) {
			Map<String, PrimitiveValue> temp = this.rightIterator.next();
			this.index = 0;
			if(temp != null && !temp.get(this.rightExpression).equals(this.rightTuple.get(this.rightExpression))) {
				setNextLeftIterator();
			}
			this.rightTuple = temp;
		}
		return result;
	}
	
	
	// this.rightTuple is preComputed
	public void setNextLeftIterator() {
		this.rightTupleList = new ArrayList<Map<String, PrimitiveValue>>();
		if(this.rightTuple == null) {
			return;
		}
		this.rightTupleList.add(this.rightTuple);
		Map<String, PrimitiveValue> next = this.rightIterator.next();
		
		while(next != null && this.rightTuple.get(this.rightExpression).equals(next.get(this.rightExpression))) {
			this.rightTuple = next;
			next = this.rightIterator.next();
			this.rightTupleList.add(this.rightTuple);
		}
		this.rightTuple = next;
		
	}

	@Override
	public void reset() {
		this.leftIterator.reset();
		this.rightIterator.reset();
	}
	
	public Map<String, PrimitiveValue> pushTogetherMap(Map<String, PrimitiveValue> leftTuple, Map<String, PrimitiveValue> rightTuple){
		if(leftTuple == null || rightTuple == null) {
			return null;
		}
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

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return this.leftIterator;
	}
}
