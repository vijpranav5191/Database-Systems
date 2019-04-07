package iterators;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import utils.EvaluateUtils;
import iterators.OrderByIterator;
public class newGroupBy implements DefaultIterator{

	
	DefaultIterator iterator;
	private List<SelectItem> selectItems;
	private Expression having;
	private List<Set<Map<String,PrimitiveValue>>> arrayList;
//	List<List<Map<String, PrimitiveValue>>> resultSet ;
	List<OrderByElement> ordElem;
	private int index;
	private ArrayList<Map<String,PrimitiveValue>> lstObj;
	private Set<Map<String, PrimitiveValue>> tempList;
	private ArrayList<ArrayList<Map<String, PrimitiveValue>>> resultSet;

	
	public newGroupBy(DefaultIterator iterator, List<Column> groupBy, Table primaryTable, List<SelectItem> selectItems) throws Exception {
		this.selectItems = selectItems;
		// TODO Auto-generated constructor stub
		this.iterator = iterator; 
		this.lstObj = new ArrayList<>();
		this.having = having;
		
		this.ordElem =  new ArrayList<OrderByElement>();
		for(Column col : groupBy)
		{
			OrderByElement ord = new OrderByElement();
			ord.setExpression(col);
			ordElem.add( ord);	
		}
		
//		OrderByIterator ordItr = new OrderByIterator(ordElem , iterator);
		
		while(this.iterator.hasNext())
		{	
			
			lstObj.add( this.iterator.next() );
		}
		
//		for(Map obj : lstObj)
//		{
//			System.out.println(obj); 
//		}
		arrayList = new ArrayList<Set<Map<String,PrimitiveValue>>>();
		this.resultSet = new ArrayList<>();
		this.resultSet.add(lstObj);
//		this.resultSet = getArrayList( lstObj , ordElem );
		orderDataByElement(ordElem);
		
		
//		for(List<Map<String,PrimitiveValue>> l : arrayList)
//		{
//			System.out.println(l);
//		}
		this.index = 0;
	}
	
//	while(itr.hasNext())
//	{
//		Map<String, PrimitiveValue> pmNew = itr.next();  
//		for(Column group : groupBy)
//		{
//			if( !pmNew.get(group.toString()).equals(pm.get(group.toString())) )
//			{
//				this.deItr = itr;
//				return resultList;
//			}	
//		}
//		resultList.add(pmNew);
////	}
	
	private void orderDataByElement(List<OrderByElement> ordElem) {
		for(OrderByElement orderByElement: ordElem) {
			for(ArrayList<Map<String, PrimitiveValue>> x : this.resultSet) {
				if(orderByElement.isAsc()) {
					Column col= (Column) orderByElement.getExpression();
					if(col.getTable() != null) {
						this.sortByCol(x, 1, col.toString());	
					} else {
						this.sortByCol(x, 1, col.getColumnName());		
					}
				}
			}
			this.resultSet = disIntegrateList(orderByElement);
		}
	}

	private ArrayList<ArrayList<Map<String, PrimitiveValue>>> disIntegrateList(OrderByElement orderByElement) {
		String byKey = null;
		Column col= (Column) orderByElement.getExpression();
		if(col.getTable() != null) {
			byKey = col.toString();	
		} else {
			byKey = col.getColumnName();		
		}
		ArrayList<ArrayList<Map<String, PrimitiveValue>>> temp = new ArrayList<ArrayList<Map<String, PrimitiveValue>>>();
		
		for(ArrayList<Map<String, PrimitiveValue>> list: this.resultSet) {
			Map<String, PrimitiveValue> curr = null;
			ArrayList<Map<String, PrimitiveValue>>  disIntegratedList = new ArrayList<Map<String, PrimitiveValue>>();
			for(Map<String, PrimitiveValue> element: list) {
				if(curr == null || !curr.get(byKey).equals(element.get(byKey))) {
					curr = element;
					if(disIntegratedList.size() > 0) {
						temp.add(disIntegratedList);
					}
					disIntegratedList = new ArrayList<Map<String, PrimitiveValue>>(); 
					disIntegratedList.add(curr);
				} else {
					disIntegratedList.add(element);
				}
			}
			if(disIntegratedList.size() > 0) {
				temp.add(disIntegratedList);
			}
		}
		return temp;
	}
	
	public ArrayList<Map<String, PrimitiveValue>> sortByCol(ArrayList<Map<String, PrimitiveValue>> maps, int sortDirection, String columnName){
		Comparator<Map<String, PrimitiveValue>> comp = new Comparator<Map<String, PrimitiveValue>>(){
			public int compare(Map<String, PrimitiveValue> a, Map<String, PrimitiveValue> b){
				//reverse result if DESC (sortDirection = -1)
				PrimitiveValue aValue = a.get(columnName);
				PrimitiveValue bValue = b.get(columnName);
				int value = 0;
				
				Table table = new Table();
				table.setName("R");
				
				Column aCol = new Column();
				aCol.setColumnName("A");
				aCol.setTable(table);
				
				Column bCol = new Column();
				bCol.setColumnName("B");
				bCol.setTable(table);
				
				
				GreaterThan gtt = new GreaterThan();
				gtt.setLeftExpression(aCol);
				gtt.setRightExpression(bCol);
				
				Map<String, PrimitiveValue> scope = new HashMap<String, PrimitiveValue>();
				scope.put(table.getName() + ".A", aValue);
				scope.put(table.getName() + ".B", bValue);
				
				try {
					if(aValue instanceof StringValue) {
						return aValue.toString().compareTo(bValue.toString()) * sortDirection;
					} else if(aValue instanceof DoubleValue){
						return (int) ((aValue.toDouble() - bValue.toDouble()) * sortDirection);
					} else if(aValue instanceof LongValue){
						return (int) ((aValue.toLong() - bValue.toLong()) * sortDirection);	
					}else {
						if(EvaluateUtils.evaluate(scope, gtt)) {
							value = 10;
						} else {
							value = -10;
						}
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return sortDirection * value;
			}
		};
		
		Collections.sort(maps, comp);
		return maps;
	}
	
//	private List<Set<Map<String, PrimitiveValue>>> getArrayList(ArrayList<Map<String, PrimitiveValue>> lstObj,
//			List<OrderByElement> ordElem) {
//		// TODO Auto-generated method stub
//		Iterator< Map<String,PrimitiveValue> > itr = lstObj.iterator();
//		Map<String,PrimitiveValue> pm = null;
//
//		
//		while(itr.hasNext())
//		{
//			
//			if(pm == null)
//			{	pm=itr.next();
//			}
//			this.tempList = new HashSet<Map<String,PrimitiveValue>>();
//
//			tempList.add(pm);
//			boolean flag = true;
//			while(flag && itr.hasNext())
//			{ 
//
//				Map<String,PrimitiveValue> pmNew = itr.next();			
//				for(OrderByElement group : ordElem)
//				{
//						if(!pmNew.get(group.toString()).equals(pm.get(group.toString())) )
//						{
////							System.out.println( "  " + pm + " " + pmNew);
//							pm = pmNew;
//							arrayList.add(tempList);
//							flag = false;
//						}
//						else
//						{
////							System.out.println(" " + pmNew + " "  + pm);
//							tempList.add(pmNew);
//						}
//				}
//			}
//			tempList.clear();
//		}
//		arrayList.add(tempList);
//		
//		return arrayList;
//	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
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
									Map<String, PrimitiveValue> temp = iter1.next();
									selectMap.putAll(temp);
									if(selectExpression.getAlias()!=null) {
										Set<String> keys  = temp.keySet();
										for (String string : keys) {
											selectMap.put(selectExpression.getAlias(), selectMap.get(string));
										}
									}
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
		
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return null;
	}

}
