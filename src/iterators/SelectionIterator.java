package iterators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SelectionIterator implements DefaultIterator{
	private List<SelectItem> selectItems;
	TableScanIterator iterator;
	
	SelectionIterator(TableScanIterator iterator, List<SelectItem> selectItems) {
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
		}
		return selectMap;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
