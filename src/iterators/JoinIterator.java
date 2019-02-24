package iterators;

import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class JoinIterator implements DefaultIterator{
	DefaultIterator leftIterator;
	DefaultIterator rightIterator;
	Map<String, PrimitiveValue> leftTuple;
	
	public JoinIterator(DefaultIterator leftIterator, DefaultIterator rightIterator) {
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.leftTuple = leftIterator.next();
	}
	
	@Override
	public boolean hasNext() {
		if(!this.leftIterator.hasNext() && !this.rightIterator.hasNext()) {
			return false;
		}
		return true;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		if(!rightIterator.hasNext()) {
			this.leftTuple = this.leftIterator.next();
			this.rightIterator.reset();
		}
		Map<String, PrimitiveValue> rightTuple = this.rightIterator.next();
		for(String key: this.leftTuple.keySet()) {
			rightTuple.put(key, this.leftTuple.get(key));
		}
		return rightTuple;
	}

	@Override
	public void reset() {
		this.leftIterator.reset();
		this.rightIterator.reset();
		this.leftTuple = leftIterator.next();
	}
}
