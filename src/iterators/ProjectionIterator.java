package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionIterator implements DefaultIterator{
	private List<SelectItem> selectItems;
	DefaultIterator iterator;
	List<String> columns;
	
	public ProjectionIterator(DefaultIterator iterator, List<SelectItem> selectItems) {
		this.selectItems = selectItems;
		this.iterator = iterator;
		this.columns = new ArrayList<String>();
		for(int index = 0; index < this.selectItems.size();index++) {
			SelectItem selectItem = this.selectItems.get(index);
			if(selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExpression = (SelectExpressionItem) selectItem;
				Column column = (Column) selectExpression.getExpression();
				this.columns.add(column.getTable() + "." + column.getColumnName());
			} else {
				
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
					Column column = (Column) selectExpression.getExpression();
					if(column.getTable() != null && column.getColumnName() != null) {
						selectMap.put(column.getTable() + "." + column.getColumnName(), map.get(column.getTable() + "." + column.getColumnName()));
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
