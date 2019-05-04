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
	
	List<PrimitiveValue> leftTuple;
	List<PrimitiveValue> nextResult;
	
	Join join;
	List<String> columns;
	Map<String, Integer> columnMapper;
	
	public JoinIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join) {
		this.columns = new ArrayList<String>();
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.join = join;
		this.columns.addAll(this.leftIterator.getColumns());
		this.columns.addAll(this.rightIterator.getColumns());
		createMapperColumn();
		
		this.leftTuple = leftIterator.next();
		this.nextResult = this.getNextWhereIter();
		
	}
	
	private void createMapperColumn() {
		this.columnMapper = new HashMap<String, Integer>();
		int index = 0;
		for(String col: this.columns) {
			this.columnMapper.put(col, index);
			index += 1;
		}
	}
	
	@Override
	public boolean hasNext() {
		if(this.nextResult != null) {
			return true;
		}
		return false;
	}
	
	@Override
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> temp = this.nextResult;
		this.nextResult = getNextWhereIter();
		return temp;
	}

	@Override
	public void reset() {
		this.leftIterator.reset();
		this.rightIterator.reset();
		this.leftTuple = leftIterator.next();
	}
	
	public List<PrimitiveValue> getNextIter(){
		List<PrimitiveValue> temp = new ArrayList<>();
		
		if(!this.rightIterator.hasNext()) {
			this.leftTuple = this.leftIterator.next();
			if(this.leftTuple != null) {
				this.rightIterator.reset();
			} else {
				return null;
			}
		}
		List<PrimitiveValue> rightTuple = this.rightIterator.next();
		if(this.leftTuple == null || rightTuple == null) {
			return null;
		}
		temp.addAll(leftTuple);
		temp.addAll(rightTuple);
		return temp;
	}

	
	public List<PrimitiveValue> getNextWhereIter(){
		List<PrimitiveValue> temp = this.getNextIter();
		if(this.join != null) {
			Expression exp = this.join.getOnExpression();
			if(exp != null) {
				try {
					while(temp != null && !EvaluateUtils.evaluate(temp, exp, this.columnMapper)) {
						temp = this.getNextIter();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return temp;
	}
	
	
	@Override
	public List<String> getColumns() {
		return this.columns;
	}
}
