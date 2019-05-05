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
			int index = 0;
			for(String col: this.columnMap.keySet()) {
				String type = map.get(this.columnMap.get(col)).getType().toString().toLowerCase();
				this.dataType.put(col, type);
				index += 1;
			}
		}
		String hash = "";
		this.avgCount += 1;
		for(Column col: this.groupBy) {
			hash += (map.get(this.columnMap.get(col.getTable().getName() + "." + col.getColumnName())) + "|");
		}
		hash = hash.substring(0, hash.length() - 1);
		PrimitiveValue pv = null;
		double x = 0;
		switch(this.aggregator.toUpperCase()) {
			case "SUM":
				for(Expression exp: this.expression.getExpressions()) {
					pv = EvaluateUtils.evaluateExpression(map, exp, this.columnMap);
					x += pv.toDouble() + this.accum.getOrDefault(hash, new DoubleValue(0)).toDouble();
				}
				break;
			case "AVG":
				for(Expression exp: this.expression.getExpressions()) {
					pv = EvaluateUtils.evaluateExpression(map, exp, this.columnMap);
					x += (pv.toDouble() + (this.accum.getOrDefault(hash, new DoubleValue(0)).toDouble() * (this.avgCount - 1))) / this.avgCount;
				}
				break;
			case "MIN":
				for(Expression exp: this.expression.getExpressions()) {
					pv = EvaluateUtils.evaluateExpression(map, exp, this.columnMap);
					x = Math.min(this.accum.getOrDefault(hash, new DoubleValue(999999999)).toDouble(), pv.toDouble());
				}
				break;
			case "MAX":
				for(Expression exp: this.expression.getExpressions()) {
					pv = EvaluateUtils.evaluateExpression(map, exp, this.columnMap);
					x = Math.max(this.accum.getOrDefault(hash, new DoubleValue(999999999)).toDouble(), pv.toDouble());
				}
				break;
			case "COUNT":
				x =  this.accum.getOrDefault(hash, new DoubleValue(0)).toDouble() + 1;
				break;	
		}
		this.accum.put(hash, new DoubleValue(x));
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
