package iterators;

import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;

public interface DefaultIterator {
	public boolean hasNext();
	public Map<String, PrimitiveValue> next();
	public void reset();
	public List<String> getColumns();
}
