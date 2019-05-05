package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import utils.EvaluateUtils;

public class ProjectionIterator implements DefaultIterator{
	private List<SelectItem> selectItems;
	private List<Column> groupBy;
	DefaultIterator iterator;
	List<String> columns;
	Table primaryTable;
	private boolean zeroAggflag;
	private String catchfunc;
	Map<String, Integer> columnMapper;

	public ProjectionIterator(DefaultIterator iterator, List<SelectItem> selectItems, Table primaryTable, List<Column> groupBy) {
		this.selectItems = selectItems;
		this.iterator = iterator;
		this.columns = new ArrayList<String>();
		this.primaryTable = primaryTable;
		this.groupBy = groupBy;

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
						} else {
							if(func.isAllColumns()) {
								this.columns.add("COUNT(*)");
								if(!this.iterator.hasNext()) {
									this.zeroAggflag = true;
									this.catchfunc = "COUNT(*)";
								}
							}
						}
					} else {
						if(selectExpression.getAlias() == null) {
							this.columns.add(selectItem.toString());
						} else {
							this.columns.add(selectExpression.getAlias());
						}
					}
				} else {
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
	}

	private void createMapperColumn() {
		this.columnMapper = new HashMap<String, Integer>();
		int index = 0;
		for(String col: this.iterator.getColumns()) {
			this.columnMapper.put(col, index);
			index += 1;
		}
	}
	
	@Override
	public boolean hasNext() {
		return this.iterator.hasNext();
	}

	@Override
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> map = this.iterator.next();
		List<PrimitiveValue> selectMap = new ArrayList<PrimitiveValue>();
		
		if(map == null && this.zeroAggflag) {
			this.zeroAggflag = false;
			map = new ArrayList<>();	
			map.add(new LongValue(0));
		}

		if(map != null) { // hasNext() not working

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
							if(selectExpression.getAlias() != null) {
								selectMap.add(map.get(this.columnMapper.get(column.getTable().getName() + "." + column.getColumnName())));	
							}
						} else if(column.getTable().getAlias() != null && column.getColumnName() != null) {
							selectMap.add(map.get(this.columnMapper.get(column.getTable().getAlias() + "." + column.getColumnName())));		
							if(selectExpression.getAlias() != null) {
								selectMap.add(map.get(this.columnMapper.get(column.getTable().getAlias() + "." + column.getColumnName())));		
							}
						} else if(column.getTable().getAlias() == null && column.getTable().getName() == null){
							for(String key: this.columnMapper.keySet()) {
								if(key.split("\\.")[1].equals(column.getColumnName())) {
									selectMap.add(map.get(this.columnMapper.get(key)));					
									if(selectExpression.getAlias() != null) {
										selectMap.add(map.get(this.columnMapper.get(key)));					
									}
									break;
								}
							}
						}
					} else if(selectExpression.getExpression() instanceof Function) {
						Expression exp = selectExpression.getExpression();
						Function func = (Function) exp;
						if(this.groupBy == null) {
							try {
								this.iterator.reset();
								DefaultIterator iter = new SimpleAggregateIterator(this.iterator, func, this.columnMapper);
								List<PrimitiveValue> temp = iter.next();
								selectMap.addAll(temp);
								selectMap.addAll(temp);

							} catch (Exception e) {
								e.printStackTrace();
							}
						} else {
							String name = func.getName();
							String key= null;
							List<Expression> expList = new ArrayList<>();
							if(func.getParameters()!=null) {
								expList = func.getParameters().getExpressions(); //check this later
								StringBuilder sb = new StringBuilder();
								for(Expression exp1 : expList) {
									sb.append(exp1.toString());
								}
								key = name+"("+sb.toString()+")";
							}
							else {
								if(func.isAllColumns()) {
									key = "COUNT(*)";
								}
							}
							if(selectExpression.getAlias() != null) {
								selectMap.add(map.get(this.columnMapper.get(selectExpression.getAlias())));
							} else {
								selectMap.add(map.get(this.columnMapper.get(key)));
							}
						}
					} else {
						try {
							Expression exp = selectExpression.getExpression();
							if(selectExpression.getAlias() != null) {
								selectMap.add(EvaluateUtils.evaluateExpression(map, exp, this.columnMapper));
								selectMap.add(EvaluateUtils.evaluateExpression(map, exp, this.columnMapper));	
							} else {
								selectMap.add(EvaluateUtils.evaluateExpression(map, exp, this.columnMapper));	
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
			return selectMap;
		}
		return null;
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
