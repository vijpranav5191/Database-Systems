package iterators;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class newGroupByExternal implements DefaultIterator{
	DefaultIterator iterator;
	DefaultIterator deItr;
	List<Column> groupBy;
	Table primaryTable;
	List<SelectItem> selectItems;
	private List<String> colmnValues;
	private List<PrimitiveValue> pmValues;
	Map<String, PrimitiveValue> next;
	public newGroupByExternal(DefaultIterator iterator, List<Column> groupBy, Table fromItem,
			List<SelectItem> selectItems) throws InvalidPrimitive, IOException, ParseException {
		
		this.iterator = iterator;
		this.groupBy = groupBy;
		this.primaryTable = fromItem;
		this.selectItems  = selectItems;
		
		List<OrderByElement> ordElem =  new ArrayList<OrderByElement>();
		for(Column col : groupBy)
		{
			OrderByElement ord = new OrderByElement();
			ord.setExpression(col);
			
			ordElem.add( ord);
			
		}
		
		
		this.deItr = new newExternal(iterator, ordElem, selectItems);		
		// TODO Auto-generated constructor stub
		this.next = this.deItr.next();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return this.deItr.hasNext();
	}

	@Override
//	public Map<String, PrimitiveValue> next() {
//		// TODO Auto-generated method stub
//		return null;
//	}

	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		Map<String, PrimitiveValue> selectMap = new HashMap<String, PrimitiveValue>();
		

		if(this.deItr.hasNext()) {
			ArrayList<Map<String, PrimitiveValue>> group = getArrayList( this.deItr , groupBy);
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
			
		}
		return selectMap;
		
	}

	private ArrayList<Map<String, PrimitiveValue>> getArrayList(DefaultIterator itr, List<Column> groupBy) {
		// TODO Auto-generated method stub
		ArrayList<Map<String, PrimitiveValue>> resultList = new ArrayList<Map<String,PrimitiveValue>>();
		resultList.add(this.next);
		Map<String, PrimitiveValue> prev = this.next;
		while(itr.hasNext()){
			Map<String, PrimitiveValue> pmNew = itr.next();  
			for(Column group : groupBy){
				if( !pmNew.get(group.toString()).equals(prev.get(group.toString())) ){
					this.deItr = itr;
					this.next = pmNew;
					return resultList;
				}	
			}
			resultList.add(pmNew);
		}
		return resultList;
	}
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.deItr.reset();
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		return this.deItr.getColumns();

	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return this.iterator;
	}
	

}
