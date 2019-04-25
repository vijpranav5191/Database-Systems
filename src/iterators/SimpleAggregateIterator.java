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
	Iterator<Map<String,PrimitiveValue>> iterator2;
	Map<String, PrimitiveValue> result = new HashMap<>();
	Map<String, PrimitiveValue> map = new HashMap<>();
	public SimpleAggregateIterator(DefaultIterator iterator, Function func) {
		// TODO Auto-generated constructor stub
		this.iterator = iterator;
		String name = func.getName().toUpperCase();
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
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
						sum = sum+pv.toDouble();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			if(pv instanceof LongValue) {
				result.put(key ,new LongValue((long) sum));

			}
			else if(pv instanceof DoubleValue) {
				result.put(key ,new DoubleValue(sum));
			}
			break;
		case "AVG":
			long sum2 = 0,count=0;
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
						sum2 = sum2+ pv.toLong();
						count++;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			double res = (double)sum2/count;
			result.put(key,new DoubleValue(res));
			break;
		case "MIN":
			PrimitiveValue min = new DoubleValue(999999999);
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
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
					result.put(key ,new LongValue(min.toLong()));
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			else if(pv instanceof DoubleValue) {
				result.put(key ,min);
			}
			break;
		case "MAX":
			PrimitiveValue max = new DoubleValue(-999999999);
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
							if(pv.toDouble() > max.toDouble()) {
								max = pv;
							}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			if(pv instanceof LongValue) {
				try {
					result.put(key ,new LongValue(max.toLong()));
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			else if(pv instanceof DoubleValue) {
				result.put(key ,max);
			}
			break;
			
		case "COUNT":
			int counter =0;
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				counter++;
			}
			result.put(key,new LongValue(counter));
			break;
		default:
			break;
		}
	}
	public SimpleAggregateIterator(Iterator iter, Function func) {
		// TODO Auto-generated constructor stub
		this.iterator2 = iter;
		String name = func.getName();
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
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
						sum = sum+pv.toDouble();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			if(pv instanceof LongValue) {
				result.put(key ,new LongValue((long) sum));

			}
			else if(pv instanceof DoubleValue) {
				result.put(key ,new DoubleValue(sum));
			}
			break;
		case "AVG":
			double sum2 = 0,count=0;
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
						sum2 = sum2+ pv.toDouble();
						count++;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			double res = (double)sum2/count;
			result.put(key,new DoubleValue(res));
			break;
		case "MIN":
			PrimitiveValue min = new DoubleValue(999999999);
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
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
					result.put(key ,new LongValue(min.toLong()));
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			else if(pv instanceof DoubleValue) {
				result.put(key ,min);
			}
			break;
		case "MAX":
			PrimitiveValue max = new DoubleValue(-999999999);
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				for (int i = 0; i < expList.size(); i++) {
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
							if(pv.toDouble() > max.toDouble()) {
								max = pv;
							}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			if(pv instanceof LongValue) {
				try {
					result.put(key ,new LongValue(max.toLong()));
				} catch (InvalidPrimitive e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			else if(pv instanceof DoubleValue) {
				result.put(key ,max);
			}
			break;
		case "COUNT":
			int counter =0;
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				counter++;
			}
			result.put(key,new LongValue(counter));
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
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		Map<String, PrimitiveValue> temp = result;
		result = null;
		return temp;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.iterator.reset();
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return this.iterator;
	}

}
