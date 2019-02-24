package iterators;

import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ResultIterator implements DefaultIterator{
	DefaultIterator iterator;
	
	public ResultIterator(DefaultIterator iterator) {
		this.iterator = iterator;
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return this.iterator.hasNext();
	}
	
	@Override
	public Map<String, PrimitiveValue> next() {
//		Map<String, PrimitiveValue> pos = this.iterator.next();
//		String result = "";
//		for(String key: pos.keySet()) {
//			result += pos.get(key) + ","; 
//		}
//		return result;
		if(this.hasNext()) {
			System.out.println(this.iterator.next().toString());
		}
		return null;
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.iterator.reset();
	}

}
