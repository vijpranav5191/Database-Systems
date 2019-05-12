package iterators;

import java.util.List;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class InsertIterator implements DefaultIterator {
	
	DefaultIterator oldIterator;
	DefaultIterator updateIterator;
	public InsertIterator(DefaultIterator oldIterator , DefaultIterator updateIterator) {
		this.oldIterator = oldIterator;
		this.updateIterator = updateIterator;
	}
	
	@Override
	public boolean hasNext() {
		if(!this.oldIterator.hasNext() && !this.updateIterator.hasNext()) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public List<PrimitiveValue> next() {
		if (this.oldIterator.hasNext()) {
			return this.oldIterator.next();
		}
		
		if(this.updateIterator.hasNext()) {
			return this.updateIterator.next();
		}
		
		return null;
	}

	@Override
	public void reset() {
		this.oldIterator.reset();
		this.updateIterator.reset();

	}

	@Override
	public List<String> getColumns() {
		return this.oldIterator.getColumns();
	}

}
