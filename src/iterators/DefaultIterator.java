package iterators;

import java.util.List;

import net.sf.jsqlparser.expression.PrimitiveValue;

public interface DefaultIterator {
	public boolean hasNext();
	public List<PrimitiveValue> next();
	public void reset();
	public List<String> getColumns();
}
