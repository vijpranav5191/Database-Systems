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
import utils.EvaluateUtils;

public class SimpleAggregateIterator implements DefaultIterator {
	DefaultIterator iterator;
	Iterator<Map<String,PrimitiveValue>> iterator2;
	Map<String, PrimitiveValue> result = new HashMap<>();
	Map<String, PrimitiveValue> map = new HashMap<>();
	public SimpleAggregateIterator(DefaultIterator iterator, Function func) {
		// TODO Auto-generated constructor stub
		this.iterator = iterator;
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
		switch (name) {
		case "SUM":
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					
					PrimitiveValue pv;
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
						if(pv instanceof LongValue) {
							long sum = 0;
							sum = sum+pv.toLong();
							result.put(key ,new LongValue(sum));
						}
						else if(pv instanceof DoubleValue) {
							double sum = 0;
							sum = sum + pv.toDouble();
							result.put(key ,new DoubleValue(sum));
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			break;
		case "AVG":
			long sum2 = 0,count=0;
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					
					PrimitiveValue pv;
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
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					
					PrimitiveValue pv;
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
						if(pv instanceof LongValue) {
							PrimitiveValue min = new LongValue(999999999);
							if(pv.toLong()<min.toLong()) {
								min = pv;
								result.put(key,min);
							}
						}
						else if(pv instanceof DoubleValue) {
							PrimitiveValue min = new LongValue(999999999);
							if(pv.toDouble() < min.toDouble()) {
								min = pv;
								result.put(key,min);
							}
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			break;
		case "MAX":
			while(this.iterator.hasNext()) {
				map = this.iterator.next();
				for (int i = 0; i < expList.size(); i++) {
					
					PrimitiveValue pv;
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
						if(pv instanceof LongValue) {
							PrimitiveValue max = new LongValue(999999999);
							if(pv.toLong()>max.toLong()) {
								max = pv;
								result.put(key,max);
							}
						}
						else if(pv instanceof DoubleValue) {
							PrimitiveValue max = new LongValue(999999999);
							if(pv.toDouble() > max.toDouble()) {
								max = pv;
								result.put(key,max);
							}
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
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
		switch (name) {
		case "SUM":
			long sum = 0;
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				for (int i = 0; i < expList.size(); i++) {
					
					PrimitiveValue pv;
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
						sum = sum+ pv.toLong();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			result.put(key ,new LongValue(sum));
			break;
		case "AVG":
			long sum2 = 0,count=0;
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				for (int i = 0; i < expList.size(); i++) {
					
					PrimitiveValue pv;
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
			PrimitiveValue min = new LongValue(999999999);
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();
				for (int i = 0; i < expList.size(); i++) {
					
					PrimitiveValue pv;
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
						if(pv.toLong()<min.toLong()) {
							min = pv;
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			result.put(key,min);
			break;
		case "MAX":
			PrimitiveValue max = new LongValue(-999999999);
			while(this.iterator2.hasNext()) {
				map = this.iterator2.next();				
				for (int i = 0; i < expList.size(); i++) {
					
					PrimitiveValue pv;
					try {
						pv = EvaluateUtils.evaluateExpression(map, (Expression) expList.get(i));
						if(pv.toLong()>max.toLong()) {
							max = pv;
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
			result.put(key,max);
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
