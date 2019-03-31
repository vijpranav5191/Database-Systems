package iterators;

import java.util.*;
//import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import utils.EvaluateUtils;

public class orderIterator implements DefaultIterator {

	private List<OrderByElement> orderBy;
	DefaultIterator iterator;
	Table primaryTable;
	private Expression whereExp;
//	PriorityQueue<DefaultIterator> pq;
	List<Map<String, PrimitiveValue>> resultSet;
	int index;

	Map<String, PrimitiveValue> nextResult;

	public orderIterator() {

	}

	public orderIterator(DefaultIterator iterator, List<OrderByElement> orderBy) {
		// TODO Auto-generated constructor stub
		this.iterator = iterator;
		List<Map<String, PrimitiveValue>> lstObj = new ArrayList<>();
		while (this.iterator.hasNext()) {
			lstObj.add(this.iterator.next());
		}
		//System.out.println(orderBy);
		//System.out.println(lstObj.get(0));
		resultSet = backTrack(lstObj, orderBy);
		//System.out.println(" results " + resultSet);
		index = 0;
	}

	public List<Map<String, PrimitiveValue>> backTrack(List<Map<String, PrimitiveValue>> lstObj,
			List<OrderByElement> orderBy2) {
		// TODO Auto-generated method stub
		List<Map<String, PrimitiveValue>> temp = new ArrayList<>();
		// temp for result
		// 3rd argument is index starting from 0 to traverse through orderby element

		backTrackUtil(lstObj, orderBy2, 0, temp);
		return temp;

//		return null;
	}

	private void backTrackUtil(List<Map<String, PrimitiveValue>> lstObj, List<OrderByElement> orderBy2, int i,
			List<Map<String, PrimitiveValue>> res) {
		// TODO Auto-generated method stub

		if (i == orderBy2.size()) {
			for (Map<String, PrimitiveValue> map : lstObj) {
				res.add(map);
			}
//			System.out.println(" res " + res); 

			return;
		}
		// for String
//		System.out.println( i + " " + orderBy2 + " "  + lstObj + " " + String.valueOf(lstObj.get(0).get(String.valueOf(orderBy2.get(i))).getType()) ); 

		//
		if ((String.valueOf(lstObj.get(0).get(String.valueOf(orderBy2.get(i))).getType())).equals("STRING")) {
//	        System.out.println(" XYZ ");
			Map<String, List<Map<String, PrimitiveValue>>> mapRes = new TreeMap<>();
			if (i < orderBy2.size()) {

				OrderByElement key = orderBy2.get(i);
//	                System.out.println(" " + key);
				for (Map<String, PrimitiveValue> l : lstObj) {
//	                	System.out.println( " " + l.get(String.valueOf(key)) + " " + l + " " + key); 
					if (!mapRes.containsKey(String.valueOf(String.valueOf(l.get(String.valueOf(key)))))) {
						mapRes.put(String.valueOf(String.valueOf(l.get(String.valueOf(key)))), new ArrayList<>());
					}
					mapRes.get((String.valueOf(String.valueOf(l.get(String.valueOf(key)))))).add(l);
				}
			}

			for (String n : mapRes.keySet()) {
				List<Map<String, PrimitiveValue>> temp = mapRes.get(n);
				backTrackUtil(temp, orderBy2, i + 1, res);
			}
		}

		// INTEGER

		if ((String.valueOf(lstObj.get(0).get(String.valueOf(orderBy2.get(i))).getType())).equals("INTEGER")) {
//	        System.out.println(" XYZ ");
			Map<Integer, List<Map<String, PrimitiveValue>>> mapRes = new TreeMap<>();
			if (i < orderBy2.size()) {

				OrderByElement key = orderBy2.get(i);
//	                System.out.println(" " + key);
				for (Map<String, PrimitiveValue> l : lstObj) {
//	                	System.out.println( " " + l.get(String.valueOf(key)) + " " + l + " " + key); 
					if (!mapRes.containsKey(Integer.valueOf(String.valueOf(l.get(String.valueOf(key)))))) {
						mapRes.put(Integer.valueOf(String.valueOf(l.get(String.valueOf(key)))), new ArrayList<>());
					}
					mapRes.get((Integer.valueOf(String.valueOf(l.get(String.valueOf(key)))))).add(l);
				}
			}

			for (Integer n : mapRes.keySet()) {
				List<Map<String, PrimitiveValue>> temp = mapRes.get(n);
				backTrackUtil(temp, orderBy2, i + 1, res);
			}
		}

		// Double
		if ((String.valueOf(lstObj.get(0).get(String.valueOf(orderBy2.get(i))).getType())).equals("DOUBLE")) {
//	        System.out.println(" XYZ ");
			Map<Double, List<Map<String, PrimitiveValue>>> mapRes = new TreeMap<>();
			if (i < orderBy2.size()) {

				OrderByElement key = orderBy2.get(i);
//	                System.out.println(" " + key);
				for (Map<String, PrimitiveValue> l : lstObj) {
					System.out.println(" " + l.get(String.valueOf(key)).getType().toString() + " " + l + " " + key);
					if (!mapRes.containsKey(Double.valueOf(String.valueOf(l.get(String.valueOf(key)))))) {
						mapRes.put(Double.valueOf(String.valueOf(l.get(String.valueOf(key)))), new ArrayList<>());
					}
					mapRes.get((Double.valueOf(String.valueOf(l.get(String.valueOf(key)))))).add(l);
				}
			}

			for (Double n : mapRes.keySet()) {
				List<Map<String, PrimitiveValue>> temp = mapRes.get(n);
				backTrackUtil(temp, orderBy2, i + 1, res);
			}
		}

		// Long
		if ((String.valueOf(lstObj.get(0).get(String.valueOf(orderBy2.get(i))).getType())).equals("LONG")) {
//	        System.out.println(" XYZ ");
			Map<Long, List<Map<String, PrimitiveValue>>> mapRes = new TreeMap<>();
			if (i < orderBy2.size()) {

				OrderByElement key = orderBy2.get(i);
//	                System.out.println(" " + key);
				for (Map<String, PrimitiveValue> l : lstObj) {

//					System.out.println(" upar " + " " + l + " " + key);
//					System.out.println(" " + l.get(String.valueOf(key)).getType().toString() + " " + l + " " + key);

					if (!mapRes.containsKey(Long.valueOf(String.valueOf(l.get(String.valueOf(key)))))) {
						mapRes.put(Long.valueOf(String.valueOf(l.get(String.valueOf(key)))), new ArrayList<>());
					}
					mapRes.get((Long.valueOf(String.valueOf(l.get(String.valueOf(key)))))).add(l);
				}
			}

			for (Long n : mapRes.keySet()) {
				List<Map<String, PrimitiveValue>> temp = mapRes.get(n);
				backTrackUtil(temp, orderBy2, i + 1, res);
			}
		}
	}

	@Override
	public boolean hasNext() {
		return index < resultSet.size();
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> temp = null;
		if (this.index < this.resultSet.size()) {
			temp = this.resultSet.get(index);
			this.index++;
		}
		return temp;
	}

	@Override
	public void reset() {
		this.index = 0;
	}

	@Override
	public List<String> getColumns() {
		return this.iterator.getColumns();
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return this.iterator;
	}

}
