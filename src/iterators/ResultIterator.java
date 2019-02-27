package iterators;

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
				for (String string : strlist) {
					System.out.print(map.get(string)+"|");
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

}
