package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import utils.EvaluateUtils;

public class ProjectionIterator implements DefaultIterator{
	private List<SelectItem> selectItems;
	DefaultIterator iterator;
	List<String> columns;
	Table primaryTable;
	
	public ProjectionIterator(DefaultIterator iterator, List<SelectItem> selectItems, Table primaryTable) {
		this.selectItems = selectItems;
		this.iterator = iterator;
		this.columns = new ArrayList<String>();
		this.primaryTable = primaryTable;
		
		for(int index = 0; index < this.selectItems.size();index++) {
			SelectItem selectItem = this.selectItems.get(index);
			
			if(selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExpression = (SelectExpressionItem) selectItem;
				if(selectExpression.getExpression() instanceof Column) {
					Column column = (Column) selectExpression.getExpression();
					if(column.getTable().getName() != null) {
						this.columns.add(column.getTable().getName() + "." + column.getColumnName());
					} else if(column.getTable().getAlias() != null) {
						this.columns.add(column.getTable().getAlias() + "." + column.getColumnName());
					} else {
						this.columns.add(column.getColumnName());	
					}
				} else {
					this.columns.add(selectExpression.getAlias());
				}
			} else if(selectItem instanceof AllTableColumns){
				AllTableColumns allTableColumns = (AllTableColumns) selectItem;
				Table table = allTableColumns.getTable();
				for(String column: this.iterator.getColumns()) {
					if(column.split("\\.")[0].equals(table.getName())) {
						this.columns.add(column);
					}
				}
			}
		}
	}
	
	@Override
	public boolean hasNext() {
		return this.iterator.hasNext();
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> selectMap = new HashMap<String, PrimitiveValue>();
		Map<String, PrimitiveValue> map = this.iterator.next();
		
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
							selectMap.put(column.getTable().getName() + "." + column.getColumnName(), map.get(column.getTable().getName() + "." + column.getColumnName()));
						} else if(column.getTable().getAlias() != null && column.getColumnName() != null) {
							selectMap.put(column.getTable().getAlias() + "." + column.getColumnName(), map.get(column.getTable().getAlias() + "." + column.getColumnName()));		
						} else if(column.getTable().getAlias() == null && column.getTable().getName() == null){
							for(String key: map.keySet()) {
								if(key.split("\\.")[1].equals(column.getColumnName())) {
									selectMap.put(key.split("\\.")[1], map.get(key));					
									break;
								}
							}
						}
					} else {
						try {
							Expression exp = selectExpression.getExpression();
							selectMap.put(selectExpression.getAlias(), EvaluateUtils.evaluateExpression(map, exp));
						} catch (Exception e) {
							// TODO Auto-generated catch block
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
