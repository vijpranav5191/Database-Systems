package iterators;

import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;

public class NlJoiniterator implements DefaultIterator {
	private DefaultIterator iter, iter2;
	List<List<PrimitiveValue>> data;
	public NlJoiniterator(DefaultIterator iter, Join join) {
		// TODO Auto-generated constructor stub
		this.iter = iter;
		if(join.getRightItem()!= null) {
			Table tab = (Table) join.getRightItem();
			DefaultIterator iter2 = new TableScanIterator(tab);
		}
		
	}
	public void open() {
		
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if (!iter.hasNext() || !iter2.hasNext()) {
			return false;
		}
		else return true;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
