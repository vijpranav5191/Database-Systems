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
import net.sf.jsqlparser.statement.select.SelectItem;
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
	List<SelectItem> selectItem;
	Map<String, PrimitiveValue> rightNextTuple;
	Map<String, PrimitiveValue> leftNextTuple;
	
	ArrayList<Map<String, PrimitiveValue>> rightBufferList;
	ArrayList<Map<String, PrimitiveValue>> leftBufferList;
	
	
	int indexLeft = 0;
	int indexRight = 0;
	
	public SortMergeIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join, List<SelectItem> selectItem) throws Exception {
		this.join = join;
		this.selectItem = selectItem;
		this.columns = new ArrayList<String>();
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
		this.leftIterator = new newExternal(leftIterator, listLeft, this.selectItem);
		//this.leftIterator = new OrderByIterator(listLeft, leftIterator);
			
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
		this.rightIterator = new newExternal(rightIterator, listRight, this.selectItem);
		//this.rightIterator = new OrderByIterator(listRight, rightIterator);
		
		this.rightNextTuple = this.rightIterator.next();
		this.leftNextTuple = this.leftIterator.next();
		this.setBuffers();
		
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
		try {
			this.nextResult = getNextIter();
		} catch (Exception e) {
			this.nextResult = null;
			e.printStackTrace();
		}
		return temp;
	}

	public Map<String, PrimitiveValue> getNextIter() throws Exception{
		Map<String, PrimitiveValue> leftTuple = null;
		Map<String, PrimitiveValue> rightTuple = null;
		Map<String, PrimitiveValue> result = null;
		
		if(this.indexLeft < this.leftBufferList.size()) {
			leftTuple = this.leftBufferList.get(this.indexLeft);
			rightTuple = this.rightBufferList.get(this.indexRight);
			result = this.pushTogetherMap(leftTuple, rightTuple);
			this.indexRight++;
			if(this.indexRight >= this.rightBufferList.size()) {
				this.indexLeft++;
				this.indexRight = 0;
			}
		} else {
			this.indexRight = 0;
			this.indexLeft = 0;
			this.setBuffers();
			if(this.nextResult != null) {
				result = this.getNextIter();
			}
		}
		return result;
	}
	
	public void setBuffers() throws Exception {
		Map<String, PrimitiveValue> leftTuple = this.leftNextTuple;
		Map<String, PrimitiveValue> rightTuple = this.rightNextTuple;
		Map<String, PrimitiveValue> result = pushTogetherMap(leftTuple, rightTuple);
		if(leftTuple != null && rightTuple != null) {
			if(result !=null && EvaluateUtils.evaluate(result, this.equalTo)) {
				this.setRightBufferIterator();
				this.setLeftBufferIterator();
			} else if(result!=null && EvaluateUtils.evaluate(result, this.gtt)) { // left is greater
				this.setRightBufferIterator();
				this.setBuffers();
			} else {// right is greater
				this.setLeftBufferIterator();
				this.setBuffers();
			}
		} else {
			this.nextResult = null;
		}
	}
	
	
	// this.rightTuple is preComputed
	public void setRightBufferIterator() {
		this.rightBufferList = new ArrayList<Map<String, PrimitiveValue>>();
		if(this.rightNextTuple == null) {
			return;
		}
		this.rightBufferList.add(this.rightNextTuple);
		Map<String, PrimitiveValue> next = this.rightIterator.next();
		
		while(next != null && this.rightNextTuple.get(this.rightExpression).equals(next.get(this.rightExpression))) {
			this.rightNextTuple = next;
			next = this.rightIterator.next();
			this.rightBufferList.add(this.rightNextTuple);
		}
		this.rightNextTuple = next;
		
	}
	
	// this.rightTuple is preComputed
	public void setLeftBufferIterator() {
		this.leftBufferList = new ArrayList<Map<String, PrimitiveValue>>();
		if(this.leftNextTuple == null) {
			return;
		}
		this.leftBufferList.add(this.leftNextTuple);
		Map<String, PrimitiveValue> next = this.leftIterator.next();
		
		while(next != null && this.leftNextTuple.get(this.leftExpression).equals(next.get(this.leftExpression))) {
			this.leftNextTuple = next;
			next = this.leftIterator.next();
			this.leftBufferList.add(this.leftNextTuple);
		}
		this.leftNextTuple = next;
		
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
