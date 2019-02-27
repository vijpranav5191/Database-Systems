package iterators;

import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class ResultIterator implements DefaultIterator{
	DefaultIterator iterator;
	
	public ResultIterator(DefaultIterator iterator) {
		this.iterator = iterator;
	}
	
	@Override
	public boolean hasNext() {
		return this.iterator.hasNext();
	}
	
	@Override
	public Map<String, PrimitiveValue> next() {
		if(this.hasNext()) {
			Map<String, PrimitiveValue> map = this.iterator.next();
			if(map != null) {
				Set<String> strlist = map.keySet();
				int count = strlist.size();
				for (String string : strlist) {
					if(count!=1){
						System.out.print(map.get(string)+"|");
					}
					else {
						System.out.print(map.get(string));
					}
					count--;
				}
				System.out.println("");
			}
		}
		return null;
	}
	
	@Override
	public void reset() {
		this.iterator.reset();
	}

	@Override
	public List<String> getColumns() {
		return this.iterator.getColumns();
	}

}