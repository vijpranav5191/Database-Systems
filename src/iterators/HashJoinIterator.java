package iterators;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import utils.EvaluateUtils;
import utils.Utils;

//Applicable only on EquiJoins

public class HashJoinIterator implements DefaultIterator {
	DefaultIterator leftIterator;
	DefaultIterator rightIterator;
	
	Map<String, PrimitiveValue> rightTuple;
	String currentHashKey;
	String rightExpression = null;
	
	Join join;
	List<String> columns;
	
	HashMap<String, ArrayList<Map<String, PrimitiveValue>>> passMap; // In Memory left map
	
	ArrayList<Map<String, PrimitiveValue>> mapList;
	int indexMapList = 0;
	Map<String, PrimitiveValue> nextResult;
		
	public HashJoinIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join){
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.join = join;
		this.columns = new ArrayList<String>();
		this.passMap = new HashMap<String, ArrayList<Map<String, PrimitiveValue>>>();
		createOnePassHash();
		this.nextResult = getNextIter();
	}

	private void createOnePassHash() {
		if(this.join.getOnExpression() instanceof EqualsTo) {
			EqualsTo exp = (EqualsTo) this.join.getOnExpression();
			String leftExpression = exp.getLeftExpression().toString(); // need to check this approach
			String rightExpression = exp.getRightExpression().toString();
			
			while(this.leftIterator.hasNext()) {
				Map<String, PrimitiveValue> map = this.leftIterator.next();
				String key = null; 
				if(map.containsKey(leftExpression)) {
					 key = leftExpression;
					 this.rightExpression = rightExpression;
				} else if(map.containsKey(rightExpression)) {
					 key = rightExpression;
					 this.rightExpression = leftExpression;
				}
				try {
					PrimitiveValue value = map.get(key);
					String hashKey = Utils.hashString(value.toString());
					ArrayList<Map<String, PrimitiveValue>> list = this.passMap.getOrDefault(hashKey, new ArrayList<Map<String, PrimitiveValue>>());
					list.add(map);
					this.passMap.put(hashKey, list);
				} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
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
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> temp = this.nextResult;
		this.nextResult = getNextIter();
		return temp;
	}
	
	@Override
	public void reset() {
		this.leftIterator.reset();
		this.rightIterator.reset();
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
		Map<String, PrimitiveValue> temp = new HashMap<String, PrimitiveValue>();
		
		if(this.mapList!= null && this.mapList.size() > 0 && this.indexMapList < this.mapList.size()) {
			Map<String, PrimitiveValue> leftTuple = this.mapList.get(this.indexMapList);
			this.indexMapList++;
			for(String key: this.rightTuple.keySet()) {
				temp.put(key, this.rightTuple.get(key));
			}
			for(String key: leftTuple.keySet()) {
				temp.put(key, leftTuple.get(key));
			}
		} else {
			if(!this.rightIterator.hasNext()) {
				return null;
			}
			this.rightTuple = this.rightIterator.next();	
			PrimitiveValue value = this.rightTuple.get(this.rightExpression);
			String hashKey;
			try {
				hashKey = Utils.hashString(value.toString());
				this.currentHashKey = hashKey;
				this.indexMapList = 0;
				this.mapList = this.passMap.getOrDefault(hashKey, new ArrayList<Map<String, PrimitiveValue>>());
			} catch (Exception e) {
				e.printStackTrace();
			}
			temp = getNextIter();
		}
		return temp;
	}

	@Override
	public DefaultIterator getChildIter() {
		return null;
	}
}
