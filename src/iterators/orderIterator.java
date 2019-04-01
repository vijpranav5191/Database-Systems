package iterators;

import java.text.SimpleDateFormat;
import java.util.*;
//import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import utils.EvaluateUtils;
import java.util.Date;

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
//<<<<<<< HEAD
	
	public orderIterator(DefaultIterator iterator, List<OrderByElement> orderBy ) throws Exception {
		// TODO Auto-generated constructor stub
		 
		List<Map<String,PrimitiveValue>> lstObj = new ArrayList<>();
		while(iterator.hasNext())
		{	
			Map<String, PrimitiveValue> mp = iterator.next() ;
//			System.out.println(mp);
			lstObj.add(mp );	
		}
//		System.out.println(orderBy); 
//		System.out.println(lstObj.get(0));
		resultSet = backTrack(lstObj,orderBy);	
//=======
//
//	public orderIterator(DefaultIterator iterator, List<OrderByElement> orderBy) {
//		// TODO Auto-generated constructor stub
//		this.iterator = iterator;
//		List<Map<String, PrimitiveValue>> lstObj = new ArrayList<>();
//		while (this.iterator.hasNext()) {
//			lstObj.add(this.iterator.next());
//		}
//		//System.out.println(orderBy);
//		//System.out.println(lstObj.get(0));
//		resultSet = backTrack(lstObj, orderBy);
//		//System.out.println(" results " + resultSet);
//>>>>>>> ac3a31650d160c9d78c8268effa116c648aa87cb
		index = 0;
	}

	public List<Map<String, PrimitiveValue>> backTrack(List<Map<String, PrimitiveValue>> lstObj,
			List<OrderByElement> orderBy2) throws Exception {
		// TODO Auto-generated method stub
		List<Map<String, PrimitiveValue>> temp = new ArrayList<>();
		// temp for result
		// 3rd argument is index starting from 0 to traverse through orderby element
		
		backTrackUtil(lstObj, orderBy2, 0, temp);
		return temp;

//		return null;
	}

	private void backTrackUtil(List<Map<String, PrimitiveValue>> lstObj, List<OrderByElement> orderBy2, int i,List<Map<String, PrimitiveValue>> res) throws Exception {

		// TODO Auto-generated method stub

		if (i == orderBy2.size()) {
			for (Map<String, PrimitiveValue> map : lstObj)
			{
				res.add(map);
			}
//			System.out.println(" res " + res); 

			return;
		}
		// for String
//		System.out.println( i + " " + orderBy2 + " "  + lstObj + " " + String.valueOf(lstObj.get(0).get(String.valueOf(orderBy2.get(i))).getType()) ); 


//		if ( ( lstObj.get(0).get(orderBy2.get(i).toString()).getType() instanceof String) ) {
//		Long x = 5L;
//		System.out.println();
		 
//		    System.out.println( lstObj.get(0).get(orderBy2.get(i).toString()).getType() == lstObj.get(0).get(orderBy2.get(i).toString()).getType().STRING);
		String orderDesc = null;
		if(orderBy2.get(i).toString().split(" ").length == 2)
		{
			orderDesc = orderBy2.get(i).toString().split(" ")[0];
		}
		else {
			orderDesc = orderBy2.get(i).toString();
		}
		if ((String.valueOf(lstObj.get(0).get(orderDesc).getType())).equals("STRING")) {
	        Map<String, List<Map<String, PrimitiveValue>>> mapRes = null;
			if(orderBy2.get(i).isAsc())
			{
				mapRes = new TreeMap<>();
			}
			else
			{
				mapRes = new TreeMap<>(Collections.reverseOrder()); 
			}
			if (i < orderBy2.size()) {

				OrderByElement key = orderBy2.get(i);
				
//	                System.out.println(" " + key);
				for (Map<String, PrimitiveValue> l : lstObj) {
					if (!mapRes.containsKey(String.valueOf(String.valueOf(l.get(orderDesc))))) {
//						System.out.println(" here " + Long.valueOf(String.valueOf(l.get(orderDesc))));
						mapRes.put((String.valueOf(l.get(orderDesc))), new ArrayList<>());
//						System.out.println(mapRes);
					}
					mapRes.get(String.valueOf(String.valueOf(l.get(orderDesc)))).add(l);
				}
			}

			for (String n : mapRes.keySet()) {
				List<Map<String, PrimitiveValue>> temp = mapRes.get(n);
				backTrackUtil(temp, orderBy2, i + 1, res);
			}
		}

		// INTEGER

		if ((String.valueOf(lstObj.get(0).get(orderDesc).getType())).equals("INTEGER")) {
	        Map<Integer, List<Map<String, PrimitiveValue>>> mapRes = null;
			if(orderBy2.get(i).isAsc())
				mapRes = new TreeMap<>();
			else
			{
				mapRes = new TreeMap<>(Collections.reverseOrder()); 
					
			}
			
			if (i < orderBy2.size()) {

				OrderByElement key = orderBy2.get(i);
//	                System.out.println(" " + key);
				for (Map<String, PrimitiveValue> l : lstObj) {
//					System.out.println(" " + l.get(String.valueOf(key)).getType().toString() + " " + l + " " + key);
					if (!mapRes.containsKey(Long.valueOf(String.valueOf(l.get(orderDesc))))) {
						mapRes.put(Integer.valueOf(String.valueOf(l.get(orderDesc))), new ArrayList<>());
//						System.out.println(mapRes);
					}
					mapRes.get(Integer.valueOf(String.valueOf(l.get(orderDesc)))).add(l);
				}
			}

			for (Integer n : mapRes.keySet()) {
				List<Map<String, PrimitiveValue>> temp = mapRes.get(n);
				backTrackUtil(temp, orderBy2, i + 1, res);
			}
		}

		// Double
		if ((String.valueOf(lstObj.get(0).get(orderDesc).getType())).equals("DOUBLE")) {
	        Map<Double, List<Map<String, PrimitiveValue>>> mapRes = null;
			if(orderBy2.get(i).isAsc())
				mapRes = new TreeMap<>();
			else
			{
				mapRes = new TreeMap<>(Collections.reverseOrder()); 
			}
			
			if (i < orderBy2.size()) {

				OrderByElement key = orderBy2.get(i);
//	                System.out.println(" " + key);
				for (Map<String, PrimitiveValue> l : lstObj) {
//					System.out.println(" " + l.get(String.valueOf(key)).getType().toString() + " " + l + " " + key);
					if (!mapRes.containsKey(Long.valueOf(String.valueOf(l.get(orderDesc))))) {
						mapRes.put(Double.valueOf(String.valueOf(l.get(orderDesc))), new ArrayList<>());
//						System.out.println(mapRes);
					}
					mapRes.get(Double.valueOf(String.valueOf(l.get(orderDesc)))).add(l);
				}
			}

			for (Double n : mapRes.keySet()) {
				List<Map<String, PrimitiveValue>> temp = mapRes.get(n);
				backTrackUtil(temp, orderBy2, i + 1, res);
			}
		}

		// Long
		if ((String.valueOf(lstObj.get(0).get(orderDesc).getType())).equals("LONG")) {
	        Map<Long, List<Map<String, PrimitiveValue>>> mapRes = null;
			if(orderBy2.get(i).isAsc())
				mapRes = new TreeMap<>();
			else
			{
				mapRes = new TreeMap<>(Collections.reverseOrder());
			}
			
			if (i < orderBy2.size()) {
				
				OrderByElement key = orderBy2.get(i);
//	                System.out.println(" " + key);
				for (Map<String, PrimitiveValue> l : lstObj) {

					System.out.println(" " + l.get(String.valueOf(key)).getType().toString() + " " + l + " " + key);

					if (!mapRes.containsKey(Long.valueOf(String.valueOf(l.get(orderDesc))))) {
						mapRes.put(Long.valueOf(String.valueOf(l.get(orderDesc))), new ArrayList<>());
//						System.out.println(mapRes);
					}
					mapRes.get(Long.valueOf(String.valueOf(l.get(orderDesc)))).add(l);
//					System.out.println(" PLA "+ (Long.valueOf(String.valueOf(l.get(orderDesc)))) + "  " + l);
//					System.out.println(l.get(orderDesc));
//					mapRes.get((l.get(orderDesc).toLong());
				}
			}

			for (Long n : mapRes.keySet()) {
				List<Map<String, PrimitiveValue>> temp = mapRes.get(n);
				backTrackUtil(temp, orderBy2, i + 1, res);
			}
		}
		// DATE
		if ((String.valueOf(lstObj.get(0).get(orderDesc).getType())).equals("DATE")) {
	    	
			Map<Date, List<Map<String, PrimitiveValue>>> mapRes = null;
			if(orderBy2.get(i).isAsc())
				mapRes = new TreeMap<>();
			
			else
			{
				mapRes = new TreeMap<>(Collections.reverseOrder()); 

			}

			
			if (i < orderBy2.size()) {

				OrderByElement key = orderBy2.get(i);
//	                System.out.println(" " + key);
				for (Map<String, PrimitiveValue> l : lstObj) {

//					System.out.println(" upar " + " " + l + " " + key);
//					System.out.println(" " + l.get(String.valueOf(key)).getType().toString() + " " + l + " " + key);
					 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				     Date date1 = sdf.parse(String.valueOf(l.get(orderDesc)));
//				     Date date2 = sdf.parse("2010-01-31");
				     
				     
					
					if (!mapRes.containsKey(date1)) {
						mapRes.put(date1, new ArrayList<>());
					}
					mapRes.get(sdf.parse(String.valueOf(l.get(orderDesc)))).add(l);
				}
			}

			for (Date n : mapRes.keySet()) {
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
//<<<<<<< HEAD
//		return this.getColumns();
//=======
		return null;
//>>>>>>> ac3a31650d160c9d78c8268effa116c648aa87cb
	}

}