//package iterators;
//
//public class GroupByIterator {
//
//}
//

package iterators;

import java.util.*;
//import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import utils.EvaluateUtils;

public class GroupByIterator implements DefaultIterator{
	
	private List<OrderByElement> orderBy;
	DefaultIterator iterator;
	Table primaryTable;
	private Expression whereExp;
	private List<SelectItem> selectItems;
	private Expression having;
//	PriorityQueue<DefaultIterator> pq;
	List<List<Map<String,PrimitiveValue>>> resultSet;
	int index ;
	
	Map<String , PrimitiveValue> nextResult;
	
	public GroupByIterator() {
		
	}
	
	public GroupByIterator(DefaultIterator result, List<Column> groupBy, Table fromItem)
	{
		
	}
	
	public GroupByIterator(DefaultIterator iterator, List<Column> groupBy, Table primaryTable, List<SelectItem> selectItems) {
		this.selectItems = selectItems;
		// TODO Auto-generated constructor stub
		this.iterator = iterator; 
		List<Map<String,PrimitiveValue>> lstObj = new ArrayList<>();
		this.having = having;
		while(this.iterator.hasNext())
		{	
			lstObj.add( iterator.next() );
		}
		resultSet = backTrack(lstObj,groupBy);	
		index = 0;
		
	}

	public List<List<Map<String,PrimitiveValue>>> backTrack(List<Map<String, PrimitiveValue>> lstObj,
			List<Column> groupBy2) {
		// TODO Auto-generated method stub
		List<List<Map<String,PrimitiveValue>>> results = new ArrayList<>();
		List<Map<String,PrimitiveValue>> temp = new ArrayList<>();
		// temp for result
		// 3rd argument is index starting from 0 to traverse through orderby element
		backTrackUtil(lstObj, groupBy2,0,temp,results);
		return results;
		
//		return null;
	}

	private void backTrackUtil(List<Map<String, PrimitiveValue>> lstObj, List<Column> groupBy2, int i,List<Map<String, PrimitiveValue>> res , List<List<Map<String,PrimitiveValue>>> results) {
		// TODO Auto-generated method stub
		
		if(i == groupBy2.size())
		{
			for (Map<String, PrimitiveValue> map : lstObj) {
				res.add(map);
			}
			
			results.add(new ArrayList<Map<String,PrimitiveValue>>(res));
			res.clear();
//			System.out.println(" res " + res); 
			
 			return;
		}
		// for String
//		System.out.println( i + " " + orderBy2 + " "  + lstObj + " " + String.valueOf(lstObj.get(0).get(String.valueOf(orderBy2.get(i))).getType()) ); 
		
		//
		if( (String.valueOf(lstObj.get(0).get(String.valueOf(groupBy2.get(i))).getType())).equals("STRING") )
		{
//	        System.out.println(" XYZ ");
	            Map< String  , List<Map<String,PrimitiveValue> >> mapRes = new TreeMap<>(); 
	            if(i<groupBy2.size())
	            {   
					
					Column key = groupBy2.get(i);
//	                System.out.println(" " + key);
	                for(Map<String,PrimitiveValue> l : lstObj)
	                {
//	                	System.out.println( " " + l.get(String.valueOf(key)) + " " + l + " " + key); 
	                    if(!mapRes.containsKey( String.valueOf(String.valueOf(l.get(String.valueOf(key))))))
	                    {
	                        mapRes.put(String.valueOf(String.valueOf(l.get(String.valueOf(key)))), new ArrayList<>());
	                    }
	                    mapRes.get(( String.valueOf(String.valueOf(l.get(String.valueOf(key)))))).add(l);
	                }
	            }
	            
	            for(String n : mapRes.keySet())
	            {
	                List<Map<String,PrimitiveValue>> temp = mapRes.get(n);
	                backTrackUtil(temp, groupBy2, i+1 , res , results);
	            }
	    }
		
		
		// INTEGER
		
		if( (String.valueOf(lstObj.get(0).get(String.valueOf(groupBy2.get(i))).getType())).equals("INTEGER") )
		{
//	        System.out.println(" XYZ ");
	            Map< Integer  , List<Map<String,PrimitiveValue> >> mapRes = new TreeMap<>(); 
	            if(i<groupBy2.size())
	            {   
					
					Column key = groupBy2.get(i);
//	                System.out.println(" " + key);
	                for(Map<String,PrimitiveValue> l : lstObj)
	                {
//	                	System.out.println( " " + l.get(String.valueOf(key)) + " " + l + " " + key); 
	                    if(!mapRes.containsKey( Integer.valueOf(String.valueOf(l.get(String.valueOf(key))))))
	                    {
	                        mapRes.put(Integer.valueOf(String.valueOf(l.get(String.valueOf(key)))), new ArrayList<>());
	                    }
	                    mapRes.get(( Integer.valueOf(String.valueOf(l.get(String.valueOf(key)))))).add(l);
	                }
	            }
	            
	            for(Integer n : mapRes.keySet())
	            {
	                List<Map<String,PrimitiveValue>> temp = mapRes.get(n);
	                backTrackUtil(temp, groupBy2, i+1 , res,results);
	            }
	    }
		
		// Double
		if( (String.valueOf(lstObj.get(0).get(String.valueOf(groupBy2.get(i))).getType())).equals("DOUBLE") )
		{
//	        System.out.println(" XYZ ");
	            Map< Double  , List<Map<String,PrimitiveValue> >> mapRes = new TreeMap<>(); 
	            if(i<groupBy2.size())
	            {   
					
					Column key = groupBy2.get(i);
//	                System.out.println(" " + key);
	                for(Map<String,PrimitiveValue> l : lstObj)
	                {
//	                	System.out.println( " " + l.get(String.valueOf(key)) + " " + l + " " + key); 
	                    if(!mapRes.containsKey( Double.valueOf(String.valueOf(l.get(String.valueOf(key))))))
	                    {
	                        mapRes.put(Double.valueOf(String.valueOf(l.get(String.valueOf(key)))), new ArrayList<>());
	                    }
	                    mapRes.get(( Double.valueOf(String.valueOf(l.get(String.valueOf(key)))))).add(l);
	                }
	            }
	            
	            for(Double n : mapRes.keySet())
	            {
	                List<Map<String,PrimitiveValue>> temp = mapRes.get(n);
	                backTrackUtil(temp, groupBy2, i+1 , res,results);
	            }
	    }
		
		
		//  Long
		if( (String.valueOf(lstObj.get(0).get(String.valueOf(groupBy2.get(i))).getType())).equals("LONG") )
		{
//	        System.out.println(" XYZ ");
	            Map< Long  , List<Map<String,PrimitiveValue> >> mapRes = new LinkedHashMap<>(); 
	            if(i<groupBy2.size())
	            {   
					
					Column key = groupBy2.get(i);
//	                System.out.println(" " + key);
	                for(Map<String,PrimitiveValue> l : lstObj)
	                {
//	                	System.out.println( " " + l.get(String.valueOf(key)) + " " + l + " " + key); 
	                    if(!mapRes.containsKey( Long.valueOf(String.valueOf(l.get(String.valueOf(key))))))
	                    {
	                        mapRes.put(Long.valueOf(String.valueOf(l.get(String.valueOf(key)))), new ArrayList<>());
	                    }
	                    mapRes.get(( Long.valueOf(String.valueOf(l.get(String.valueOf(key)))))).add(l);
	                }
	            }
	            
	            for(Long n : mapRes.keySet())
	            {
	                List<Map<String,PrimitiveValue>> temp = mapRes.get(n);
	                backTrackUtil(temp, groupBy2, i+1 , res , results);
	            }
	    }
	}

	@Override
	public boolean hasNext() {
			return index < resultSet.size();

	}

	@Override
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		Map<String, PrimitiveValue> selectMap = new LinkedHashMap<>();
		if(this.hasNext()) {
			List<Map<String, PrimitiveValue>> group = this.resultSet.get(index++);
			Iterator iter = group.iterator();
			Map<String, PrimitiveValue> map = (Map<String, PrimitiveValue>) iter.next();
			if(map!=null) {
				for(int index = 0; index < this.selectItems.size();index++) {
					SelectItem selectItem = this.selectItems.get(index);
					
					if(selectItem instanceof AllTableColumns) {
						AllTableColumns allTableColumns = (AllTableColumns) selectItem;
						allTableColumns.getTable();
						selectMap = map;
					} else if(selectItem instanceof AllColumns) {
						AllColumns allColumns = (AllColumns) selectItem;	
						selectMap = map;
					} else if(selectItem instanceof SelectExpressionItem) {
						SelectExpressionItem selectExpression = (SelectExpressionItem) selectItem;
						if(selectExpression.getExpression() instanceof Column) {
							Column column = (Column) selectExpression.getExpression();
							if(column.getTable().getName() != null && column.getColumnName() != null) {
								selectMap.put(column.getTable().getName() + "." + column.getColumnName(), map.get(column.getTable().getName() + "." + column.getColumnName()));
							} else if(column.getTable().getAlias() != null && column.getColumnName() != null) {
								selectMap.put(column.getTable().getAlias() + "." + column.getColumnName(), map.get(column.getTable().getAlias() + "." + column.getColumnName()));		
							} else if(column.getTable().getAlias() == null && column.getTable().getName() == null){
								for(String key: map.keySet()) {
									if(key.split("\\.")[1].equals(column.getColumnName())) {
										selectMap.put(key, map.get(key));					
										break;
									}
								}
							}
						} else if(selectExpression.getExpression() instanceof Function) {
							try {
								Expression exp = selectExpression.getExpression();
								if(exp instanceof Function) {
									Function func = (Function) exp;
									iter = group.iterator();
									DefaultIterator iter1 = new SimpleAggregateIterator(iter, func);
									selectMap.putAll(iter1.next());	
								}
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
			}

			return selectMap;
		}
		else return selectMap;

	}

	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.index = 0;
		
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

