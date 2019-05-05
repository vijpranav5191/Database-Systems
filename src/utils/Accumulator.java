package utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

public class Accumulator {

	Map<String, PrimitiveValue> accum;
	ExpressionList expression;
	String aggregator;
	Map<String, Integer> columnMap;
	List<Column> groupBy;
	int avgCount = 0;
	int pointer = 0;
	Iterator<String> keySet;
	String currentHash = null;
	List<PrimitiveValue> map;
	Map<String, String> dataType;
	
	public Accumulator(ExpressionList expressionList, List<Column> groupBy, 
			String aggregator, Map<String, Integer> columnMap) {
		this.aggregator = aggregator;
		this.expression = expressionList;
		this.columnMap = columnMap;
		this.groupBy = groupBy;
		this.accum = new LinkedHashMap<String, PrimitiveValue>();
	}
	
	public void aggregate(List<PrimitiveValue> map) throws Exception {
		if(this.dataType == null) {
			this.dataType = new HashMap<String, String>();
			for(String col: this.columnMap.keySet()) {
				String type = map.get(this.columnMap.get(col)).getType().toString().toLowerCase();
				this.dataType.put(col, type);
			}
		}
		String hash = "";
		this.avgCount += 1;
		for(Column col: this.groupBy) {
			hash += (map.get(this.columnMap.get(col.getTable().getName() + "." + col.getColumnName())) + "|");
		}
		hash = hash.substring(0, hash.length() - 1);
		PrimitiveValue pv = null;
		switch(this.aggregator.toUpperCase()) {
			case "SUM":
				double sum_x = 0;
				for(Expression exp: this.expression.getExpressions()) {
					pv = EvaluateUtils.evaluateExpression(map, exp, this.columnMap);
					if(pv instanceof LongValue) {
						sum_x += pv.toLong() + this.accum.getOrDefault(hash, new LongValue(0)).toLong();
						this.accum.put(hash, new LongValue((long) sum_x));
					} else if(pv instanceof DoubleValue) {
						sum_x += pv.toDouble() + this.accum.getOrDefault(hash, new DoubleValue(0)).toDouble();
						this.accum.put(hash, new DoubleValue(sum_x));
					}
				}
				
				break;
			case "AVG":
				double avg_x = 0;
				for(Expression exp: this.expression.getExpressions()) {
					pv = EvaluateUtils.evaluateExpression(map, exp, this.columnMap);
					avg_x += (pv.toDouble() + (this.accum.getOrDefault(hash, new DoubleValue(0)).toDouble() * (this.avgCount - 1))) / this.avgCount;
				}
				this.accum.put(hash, new DoubleValue(avg_x));
				break;
			case "MIN":
				for(Expression exp: this.expression.getExpressions()) {
					pv = EvaluateUtils.evaluateExpression(map, exp, this.columnMap);
					if(pv instanceof LongValue) {
						long min_x = Math.min(this.accum.getOrDefault(hash, new LongValue(999999999)).toLong(), pv.toLong());		
						this.accum.put(hash, new LongValue(min_x));
					} else if(pv instanceof DoubleValue){
						double min_x = Math.min(this.accum.getOrDefault(hash, new DoubleValue(999999999)).toDouble(), pv.toDouble());		
						this.accum.put(hash, new DoubleValue(min_x));
					}
				}
				break;
			case "MAX":
				for(Expression exp: this.expression.getExpressions()) {
					pv = EvaluateUtils.evaluateExpression(map, exp, this.columnMap);
					if(pv instanceof LongValue) {
						long max_x = Math.max(this.accum.getOrDefault(hash, new LongValue(-999999999)).toLong(), pv.toLong());		
						this.accum.put(hash, new LongValue(max_x));
					} else if(pv instanceof DoubleValue){
						double max_x = Math.max(this.accum.getOrDefault(hash, new DoubleValue(-999999999)).toDouble(), pv.toDouble());		
						this.accum.put(hash, new DoubleValue(max_x));
					}
				}
				break;
			case "COUNT":
				long count = this.accum.getOrDefault(hash, new LongValue(0)).toLong() + 1;
				this.accum.put(hash, new LongValue(count));
				break;	
		}
	}
	
	public void commit() {
		this.keySet = this.accum.keySet().iterator();
	}
	
	public boolean hashNext() {
		return this.keySet.hasNext();
	}
	
	public String nextHash(Boolean isNeeded) {
		this.currentHash = this.keySet.next();
		if(isNeeded) {
			this.map = new ArrayList<>();
			String[] row = this.currentHash.split("\\|");
			int index = 0;
			for(Column col: this.groupBy) {
				PrimitiveValue pm;
				switch (this.dataType.get(col.getTable().getName() + "." + col.getColumnName())) {
					case "long":
						pm = new LongValue(row[index]);
						break;
					case "string":
						pm = new StringValue(row[index].replace("\'", ""));
						break;
					case "varchar":
						pm = new StringValue(row[index].replace("\'", ""));
						break;	
					case "char":
						pm = new StringValue(row[index].replace("\'", ""));
						break;
					case "decimal":
						pm = new DoubleValue(row[index]);
						break;
					case "date":
						pm = new DateValue(row[index]);
						break;
					default:
						pm = new StringValue(row[index].replace("\'", ""));
						break;
				}
				map.add(pm);
				index+=1;
			}
		}
		return this.currentHash;
	}
	
	
	public List<PrimitiveValue> getNextList(){
		return this.map;
	}
	// always call nextHash Before this
	public PrimitiveValue next() {
		return this.accum.get(this.currentHash);
	}
}
