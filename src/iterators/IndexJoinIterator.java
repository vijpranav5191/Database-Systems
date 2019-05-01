package iterators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bPlusTree.BPlusTreeBuilder;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.Join;

public class IndexJoinIterator implements DefaultIterator {
	private DefaultIterator leftIterator;
	private DefaultIterator rightIterator;
	private Join join;
	private BPlusTreeBuilder btree;
	private List<String> columns;
	public IndexJoinIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join) {
		// TODO Auto-generated constructor stub
		this.leftIterator = leftIterator;
		this.join = join;
		this.btree = new BPlusTreeBuilder(rightIterator);
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return this.leftIterator.hasNext();
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> temp = new HashMap<String, PrimitiveValue>();
		if(this.leftIterator.hasNext()) {
			Map<String, PrimitiveValue> leftTuple = leftIterator.next();
			this.rightIterator = this.btree.search(leftTuple,this.join);
			if(this.rightIterator.hasNext()) {
				Map<String, PrimitiveValue> rightTuple = this.rightIterator.next();
				for(String key: rightTuple.keySet()) {
					temp.put(key, rightTuple.get(key));
				}
				for(String key: leftTuple.keySet()) {
					temp.put(key, leftTuple.get(key));
				}
				return temp;
			}
			else {
				this.next();
			}
			
		}
		else {
			return null;
		}
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.leftIterator.reset();
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		if(this.columns.size() == 0) {
			this.columns.addAll(this.leftIterator.getColumns());
			this.columns.addAll(this.rightIterator.getColumns());
		}
		return this.columns;
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return null;
	}

}
