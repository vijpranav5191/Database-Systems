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
	private List<Set<List<PrimitiveValue>>> arrayList;
	
	List<OrderByElement> ordElem;
	private int index;
	private ArrayList<List<PrimitiveValue>> lstObj;
	private Set<List<PrimitiveValue>> tempList;
	Map<String, Integer> columnMapper;
	List<String> columns;
	private ArrayList<ArrayList<List<PrimitiveValue>>> resultSet;

	
	public newGroupBy(DefaultIterator iterator, List<Column> groupBy, Table primaryTable, List<SelectItem> selectItems) throws Exception {
		this.selectItems = selectItems;
		this.iterator = iterator; 
		this.lstObj = new ArrayList<>();	
		this.ordElem =  new ArrayList<OrderByElement>();
		this.columns = new ArrayList<>();
		
		for(int index = 0; index < this.selectItems.size();index++) {
			SelectItem selectItem = this.selectItems.get(index);

			if(selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExpression = (SelectExpressionItem) selectItem;
				if(selectExpression.getExpression() instanceof Column) {
					Column column = (Column) selectExpression.getExpression();
					if(selectExpression.getAlias() != null) {
						this.columns.add(selectExpression.getAlias());
					} else {
						if(column.getTable().getName() != null) {
							this.columns.add(column.getTable().getName() + "." + column.getColumnName());
						} else if(column.getTable().getAlias() != null) {
							this.columns.add(column.getTable().getAlias() + "." + column.getColumnName());
						} else {
							this.columns.add(column.getColumnName());	
						}
					}
				} else if((selectExpression.getExpression() instanceof Function)){
					if(selectExpression.getAlias() == null){
						Function func = (Function) selectExpression.getExpression();
						String name = func.getName();
						if(func.getParameters()!=null) {
							List<Expression> expList = func.getParameters().getExpressions();
							StringBuilder sb = new StringBuilder();
							for(Expression exp : expList) {
								sb.append(exp.toString());
							}
							this.columns.add(name+"("+sb.toString()+")");
						}
					} else {
						//this.columns.add(((SelectExpressionItem) selectItem).getExpression().toString());
						this.columns.add(selectExpression.getAlias());
					}
				}
				else {
					if(selectExpression.getAlias()==null) {
						this.columns.add(selectItem.toString());
					}else {
						this.columns.add(selectExpression.getAlias());
					}
				}	
			} else if(selectItem instanceof AllTableColumns){
				AllTableColumns allTableColumns = (AllTableColumns) selectItem;
				Table table = allTableColumns.getTable();
				for(String column: this.iterator.getColumns()) {
					if(column.split("\\.")[0].equals(table.getName())) {
						this.columns.add(column);
					}
				}
			} else if(selectItem instanceof AllColumns) {
				this.columns = this.iterator.getColumns();
			}	
		}
		createMapperColumn();
		for(Column col : groupBy){
			OrderByElement ord = new OrderByElement();
			ord.setExpression(col);
			ordElem.add( ord);	
		}
		while(this.iterator.hasNext()){	
			lstObj.add( this.iterator.next() );
		}
		arrayList = new ArrayList<Set<List<PrimitiveValue>>>();
		this.resultSet = new ArrayList<>();
		this.resultSet.add(lstObj);
		orderDataByElement(ordElem);
		this.index = 0;
	}
	
	private void createMapperColumn() {
		this.columnMapper = new HashMap<String, Integer>();
		int index = 0;
		for(String col: this.iterator.getColumns()) {
			this.columnMapper.put(col, index);
			index+=1;
		}
	}

	private void orderDataByElement(List<OrderByElement> ordElem) {
		for(OrderByElement orderByElement: ordElem) {
			for(ArrayList<List<PrimitiveValue>> x : this.resultSet) {
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

	private ArrayList<ArrayList<List<PrimitiveValue>>> disIntegrateList(OrderByElement orderByElement) {
		String byKey = null;
		Column col= (Column) orderByElement.getExpression();
		if(col.getTable() != null) {
			byKey = col.toString();	
		} else {
			byKey = col.getColumnName();		
		}
		ArrayList<ArrayList<List<PrimitiveValue>>> temp = new ArrayList<ArrayList<List<PrimitiveValue>>>();
		
		for(ArrayList<List<PrimitiveValue>> list: this.resultSet) {
			List<PrimitiveValue> curr = null;
			ArrayList<List<PrimitiveValue>>  disIntegratedList = new ArrayList<List<PrimitiveValue>>();
			for(List<PrimitiveValue> element: list) {
				if(curr == null || !curr.get(this.columnMapper.get(byKey)).equals(element.get(this.columnMapper.get(byKey)))) {
					curr = element;
					if(disIntegratedList.size() > 0) {
						temp.add(disIntegratedList);
					}
					disIntegratedList = new ArrayList<List<PrimitiveValue>>(); 
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
	
	public ArrayList<List<PrimitiveValue>> sortByCol(ArrayList<List<PrimitiveValue>> maps, int sortDirection, String columnName){
		Comparator<List<PrimitiveValue>> comp = new Comparator<List<PrimitiveValue>>(){
			public int compare(List<PrimitiveValue> a, List<PrimitiveValue> b){
				//reverse result if DESC (sortDirection = -1)
				PrimitiveValue aValue = a.get(columnMapper.get(columnName));
				PrimitiveValue bValue = b.get(columnMapper.get(columnName));
				
				List<PrimitiveValue> scope = new ArrayList<PrimitiveValue>();
				Map<String, Integer> mapper = new HashMap<>();
				scope.add(aValue);
				scope.add(bValue);
				
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
				
				mapper.put(aCol.toString(), 0);
				mapper.put(bCol.toString(), 1);
				
				try {
					if(aValue instanceof StringValue) {
						return aValue.toString().compareTo(bValue.toString()) * sortDirection;
					} else if(aValue instanceof DoubleValue){
						return (int) ((aValue.toDouble() - bValue.toDouble()) * sortDirection);
					} else if(aValue instanceof LongValue){
						return (int) ((aValue.toLong() - bValue.toLong()) * sortDirection);	
					}else {
						if(EvaluateUtils.evaluate(scope, gtt, mapper)) {
							value = 10;
						} else {
							value = -10;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return sortDirection * value;
			}
		};
		
		Collections.sort(maps, comp);
		return maps;
	}

	@Override
	public boolean hasNext() {
		return index < resultSet.size();
	}

	@Override
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> selectMap = new ArrayList<>();
		
		if(this.hasNext()) {
			List<List<PrimitiveValue>> group = this.resultSet.get(index++);
			Iterator<List<PrimitiveValue>> iter = group.iterator();
			List<PrimitiveValue> map = (List<PrimitiveValue>) iter.next();
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
								selectMap.add(map.get(this.columnMapper.get(column.getTable().getName() + "." + column.getColumnName())));
							} else if(column.getTable().getAlias() != null && column.getColumnName() != null) {
								selectMap.add(map.get(this.columnMapper.get(column.getTable().getAlias() + "." + column.getColumnName())));		
							} else if(column.getTable().getAlias() == null && column.getTable().getName() == null){
								for(String key: this.columnMapper.keySet()) {
									if(key.split("\\.")[1].equals(column.getColumnName())) {
										selectMap.add(map.get(this.columnMapper.get(key)));					
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
									DefaultIterator iter1 = new SimpleAggregateIterator(iter, func, this.columnMapper);
									List<PrimitiveValue> temp = iter1.next();
									//selectMap.addAll(temp);
									selectMap.addAll(temp);
								}
							} catch (Exception e) {
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
		this.iterator.reset();
	}

	@Override
	public List<String> getColumns() {
		return this.columns;
	}
}
