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
	
	String currentHashKey;
	int indexMapList = 0;
	
	String rightExpression = null;
	Join join;
	List<String> columns;
	Map<String, Integer> columnMapper;
	Map<String, Integer> rightColumnMapper;
	Map<String, Integer> leftColumnMapper;

	
	ArrayList<List<PrimitiveValue>> mapList;
	HashMap<String, ArrayList<List<PrimitiveValue>>> passMap; // In Memory left map
	List<PrimitiveValue> rightTuple;
	List<PrimitiveValue> nextResult;
		
	public HashJoinIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join){
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.join = join;
		this.columns = new ArrayList<String>();
		this.columns.addAll(this.leftIterator.getColumns());
		this.columns.addAll(this.rightIterator.getColumns());
		createMapperColumn();
		this.passMap = new HashMap<String, ArrayList<List<PrimitiveValue>>>();
		createOnePassHash();
		this.nextResult = getNextIter();
	}

	private void createMapperColumn() {
		this.columnMapper = new HashMap<String, Integer>();
		int index = 0;
		for(String col: this.columns) {
			this.columnMapper.put(col, index);
			index+=1;
		}
		this.leftColumnMapper = new HashMap<String, Integer>();
		index = 0;
		for(String col: this.leftIterator.getColumns()) {
			this.leftColumnMapper.put(col, index);
			index+=1;
		}
		this.rightColumnMapper = new HashMap<String, Integer>();
		index = 0;
		for(String col: this.rightIterator.getColumns()) {
			this.rightColumnMapper.put(col, index);
			index+=1;
		}	
	}
	
	private void createOnePassHash() {
		if(this.join.getOnExpression() instanceof EqualsTo) {
			EqualsTo exp = (EqualsTo) this.join.getOnExpression();
			String leftExpression = exp.getLeftExpression().toString(); // need to check this approach
			String rightExpression = exp.getRightExpression().toString();
			
			while(this.leftIterator.hasNext()) {
				List<PrimitiveValue> map = this.leftIterator.next();
				
				String key = null; 
				if(this.leftColumnMapper.containsKey(leftExpression)) {
					 key = leftExpression;
					 this.rightExpression = rightExpression;
				} else if(this.leftColumnMapper.containsKey(rightExpression)) {
					 key = rightExpression;
					 this.rightExpression = leftExpression;
				}
				try {
					PrimitiveValue value = map.get(this.leftColumnMapper.get(key));
					String hashKey = Utils.hashString(value.toString());
					ArrayList<List<PrimitiveValue>> list = this.passMap.getOrDefault(hashKey, new ArrayList<List<PrimitiveValue>>());
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
		this.passMap = null;
		if(mapList != null && mapList.size() > 0) {
			this.mapList = null;
		}
		return false;
	}

	@Override
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> temp = this.nextResult;
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
		return this.columns;
	}
	
	public List<PrimitiveValue> getNextIter(){
		List<PrimitiveValue> temp = new ArrayList<PrimitiveValue>();
		
		if(this.mapList!= null && this.mapList.size() > 0 && this.indexMapList < this.mapList.size()) {
			List<PrimitiveValue> leftTuple = this.mapList.get(this.indexMapList);
			this.indexMapList++;
			temp.addAll(leftTuple);
			temp.addAll(rightTuple);
		} else {
			if(!this.rightIterator.hasNext()) {
				return null;
			}
			this.rightTuple = this.rightIterator.next();	
			PrimitiveValue value = this.rightTuple.get(this.rightColumnMapper.get(this.rightExpression));
			String hashKey;
			try {
				hashKey = Utils.hashString(value.toString());
				this.currentHashKey = hashKey;
				this.indexMapList = 0;
				this.mapList = this.passMap.getOrDefault(hashKey, new ArrayList<List<PrimitiveValue>>());
			} catch (Exception e) {
				e.printStackTrace();
			}
			temp = getNextIter();
		}
		return temp;
	}
}
