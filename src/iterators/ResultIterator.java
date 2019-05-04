package iterators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class ResultIterator implements DefaultIterator{
	DefaultIterator iterator;
	List<String> columns;
	Map<String, Integer> columnMapper;
	
	public ResultIterator(DefaultIterator iterator) {
		this.iterator = iterator;
		this.columns = iterator.getColumns();
		createMapperColumn();
	}
	
	@Override
	public boolean hasNext() {
		return this.iterator.hasNext();
	}
	
	@Override
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> map = this.iterator.next();
		if(map != null) {
			List<String> columns = this.iterator.getColumns();
			int count = columns.size();
			for (String string : columns) {
				if(count != 1){
					System.out.print(map.get(columnMapper.get(string)) + "|");
				}
				else {
					System.out.print(map.get(columnMapper.get(string)));
				}
				count--;
			}
			System.out.println("");
		}
		return map;
	}
	
	@Override
	public void reset() {
		this.iterator.reset();
	}

	@Override
	public List<String> getColumns() {
		return this.columns;
	}


	private void createMapperColumn() {
		this.columnMapper = new HashMap<String, Integer>();
		int index = 0;
		for(String col: this.columns) {
			this.columnMapper.put(col, index);
			index+=1;
		}
	}
}
