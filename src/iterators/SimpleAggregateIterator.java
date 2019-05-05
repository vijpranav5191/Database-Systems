package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import utils.EvaluateUtils;

public class SimpleAggregateIterator implements DefaultIterator {
	DefaultIterator iterator;
	Iterator<List<PrimitiveValue>> iterator2;
	List<PrimitiveValue> result = new ArrayList<>();
	List<PrimitiveValue> map = new ArrayList<>();
	Map<String, Integer> columnMapper;
	
	public SimpleAggregateIterator(DefaultIterator iterator, Function func, Map<String, Integer> columnMapper) {
		this.iterator = iterator;
		String name = func.getName().toUpperCase();
		this.columnMapper = columnMapper;
		String key= null;
		List<Expression> expList = new ArrayList<>();
		if(func.getParameters()!=null) {
			expList = func.getParameters().getExpressions(); //check this later
			StringBuilder sb = new StringBuilder();
			for(Expression exp : expList) {
				sb.append(exp.toString());
			}
			key = name+"("+sb.toString()+")";
		}
		else {
			if(func.isAllColumns()) {
				key = "COUNT(*)";
			}
		}
		PrimitiveValue pv = null	;
		switch (name) {
		case "SUM":
			double sum = 0;
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i), this.columnMapper);
						sum = sum+pv.toDouble();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			if(pv instanceof LongValue) {
				result.add(new LongValue((long) sum));

			}
			else if(pv instanceof DoubleValue) {
				result.add(new DoubleValue(sum));
			}
			break;
		case "AVG":
			double sum2 = 0,count=0;
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i), this.columnMapper);
						sum2 = sum2+ pv.toDouble();
						count++;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			double res = (double)sum2/count;
			result.add(new DoubleValue(res));
			break;
		case "MIN":
			PrimitiveValue min = new DoubleValue(999999999);
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i),this.columnMapper);
						if(pv.toDouble() < min.toDouble()) {
								min = pv;
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			if(pv instanceof LongValue) {
				try {
					result.add(new LongValue(min.toLong()));
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			else if(pv instanceof DoubleValue) {
				result.add(min);
			}
			break;
		case "MAX":
			PrimitiveValue max = new DoubleValue(-999999999);
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i),this.columnMapper);
							if(pv.toDouble() > max.toDouble()) {
								max = pv;
							}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			if(pv instanceof LongValue) {
				try {
					result.add(new LongValue(max.toLong()));
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			else if(pv instanceof DoubleValue) {
				result.add(max);
			}
			break;
			
		case "COUNT":
			int counter =0;
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				counter++;
			}
			result.add(new LongValue(counter));
			break;
		default:
			break;
		}
	}
	
	public SimpleAggregateIterator(Iterator iter, Function func, Map<String, Integer> columnMapper) {
		this.iterator2 = iter;
		String name = func.getName();
		this.columnMapper = columnMapper;
		String key= null;
		List<Expression> expList = new ArrayList<>();
		if(func.getParameters()!=null) {
			expList = func.getParameters().getExpressions(); //check this later
			StringBuilder sb = new StringBuilder();
			for(Expression exp : expList) {
				sb.append(exp.toString());
			}
			key = name+"("+sb.toString()+")";
		}
		else {
			if(func.isAllColumns()) {
				key = "COUNT(*)";
			}
		}
		PrimitiveValue pv = null	;
		switch (name) {
		case "SUM":
			double sum = 0;
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i), this.columnMapper);
						sum = sum+pv.toDouble();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			if(pv instanceof LongValue) {
				result.add(new LongValue((long) sum));

			}
			else if(pv instanceof DoubleValue) {
				result.add(new DoubleValue(sum));
			}
			break;
		case "AVG":
			double sum2 = 0,count=0;
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i), this.columnMapper);
						sum2 = sum2+ pv.toDouble();
						count++;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			double res = (double)sum2/count;
			result.add(new DoubleValue(res));
			break;
		case "MIN":
			PrimitiveValue min = new DoubleValue(999999999);
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i), this.columnMapper);
						if(pv.toDouble() < min.toDouble()) {
								min = pv;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}	
				}
			}
			if(pv instanceof LongValue) {
				try {
					result.add(new LongValue(min.toLong()));
				} catch (InvalidPrimitive e) {
					e.printStackTrace();
				}

			}
			else if(pv instanceof DoubleValue) {
				result.add(min);
			}
			break;
		case "MAX":
			PrimitiveValue max = new DoubleValue(-999999999);
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i), this.columnMapper);
							if(pv.toDouble() > max.toDouble()) {
								max = pv;
							}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			if(pv instanceof LongValue) {
				try {
					result.add(new LongValue(max.toLong()));
				} catch (InvalidPrimitive e) {
					e.printStackTrace();
				}

			}
			else if(pv instanceof DoubleValue) {
				result.add(max);
			}
			break;
		case "COUNT":
			int counter =0;
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				counter++;
			}
			result.add(new LongValue(counter));
			break;
		default:
			break;
		}
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if(result==null) return false;
		else return true;
	}

	@Override
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> temp = result;
		result = null;
		return temp;
	}

	@Override
	public void reset() {
		this.iterator.reset();
	}

	@Override
	public List<String> getColumns() {
		return null;
	}
}
