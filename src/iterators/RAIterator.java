package iterators;

import java.util.List;

public interface RAIterator {
	public boolean hasNext();
	public String next();
	public void reset();
	public List<String> getColumns();
}
