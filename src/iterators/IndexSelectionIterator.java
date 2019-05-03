package iterators;

import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;


public class IndexSelectionIterator implements DefaultIterator{
	
	DefaultIterator iterator;
	DefaultIterator iteratorTemp;
	List<ColumnDefinition> cdefs;
	
	public IndexSelectionIterator( DefaultIterator iterator, Table table , String columnName) {
			
		
	}
	
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return this.iterator.hasNext();
//		return false;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return this.iterator.next();
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.iterator = this.iteratorTemp;
		
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		
		return this.iterator.getColumns();
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return null;
	}

}
