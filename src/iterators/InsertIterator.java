package iterators;

import java.util.List;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class InsertIterator implements DefaultIterator {
	
	DefaultIterator oldIterator;
	DefaultIterator updateIterator;
	public InsertIterator(DefaultIterator oldIterator, DefaultIterator updateIterator) {
		// TODO Auto-generated constructor stub
		this.oldIterator = oldIterator;
		this.updateIterator = updateIterator;
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if(!this.oldIterator.hasNext() && !this.updateIterator.hasNext()) {
			return false;
		}
		else {
			return true;
		}
	}

	@Override
	public List<PrimitiveValue> next() {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		this.oldIterator.reset();
		this.updateIterator.reset();

	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		return this.oldIterator.getColumns();
	}

}
