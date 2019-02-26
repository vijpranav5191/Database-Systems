package iterators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionIterator implements DefaultIterator{
	private List<SelectItem> selectItems;
	DefaultIterator iterator;
	
	public ProjectionIterator(DefaultIterator iterator, List<SelectItem> selectItems) {
		this.selectItems = selectItems;
		this.iterator = iterator;
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return this.iterator.hasNext();
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> selectMap = new HashMap<String, PrimitiveValue>();
		Map<String, PrimitiveValue> map = this.iterator.next();
		for(int index = 0; index < this.selectItems.size();index++) {
			SelectItem selectItem = this.selectItems.get(index);
			if(selectItem instanceof AllTableColumns) {
				AllTableColumns allTableColumns = (AllTableColumns) selectItem;
				allTableColumns.getTable();
			} else if(selectItem instanceof AllColumns) {
				AllColumns allTableColumns = (AllColumns) selectItem;
			}
		}
		return selectMap;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
