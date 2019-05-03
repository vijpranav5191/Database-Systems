package iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bPlusTree.BPlusTreeBuilder;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.Join;
import objects.ColumnDefs;
import objects.SchemaStructure;

public class IndexJoinIterator implements DefaultIterator {
	private DefaultIterator nonIndexedIterator;
	private DefaultIterator indexedIterator;
	private DefaultIterator searchIndexIterator;
	
	private Join join;
	private BPlusTreeBuilder btree;
	private List<String> columns;
	Column indexedColumn, nonIndexedColumn;
	String indexedColumnStr, nonIndexedColumnStr;
	private Map<String, PrimitiveValue> nonIndexedTuple;
	Map<String, PrimitiveValue> nextResult;
	
	
	public IndexJoinIterator(DefaultIterator nonIndexedIterator, DefaultIterator indexedIterator, 
			Join join, Column nonIndexedColumn, Column indexedColumn) {
		this.indexedIterator = indexedIterator;
		this.nonIndexedIterator = nonIndexedIterator;
		this.columns = new ArrayList<String>();
		this.join = join;
		if(this.columns.size() == 0) {
			this.columns.addAll(nonIndexedIterator.getColumns());
			this.columns.addAll(indexedIterator.getColumns());
		}
		this.indexedColumn = indexedColumn;
		this.nonIndexedColumn = nonIndexedColumn;
		this.btree = SchemaStructure.bTreeMap.get(indexedColumn.getTable().getName());
		this.nonIndexedColumnStr =  this.nonIndexedColumn.getTable().getName() + "." + this.nonIndexedColumn.getColumnName();
		this.indexedColumnStr =  this.indexedColumn.getTable().getName() + "." + this.indexedColumn.getColumnName();	
		this.nextResult = getNextIter();
	}


	@Override
	public boolean hasNext() {
		if(this.nextResult != null) {
			return true;
		}
		return false;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> temp = this.nextResult;
		this.nextResult = getNextIter();
		return temp;
	}


	public Map<String, PrimitiveValue> getNextIter(){
		Map<String, PrimitiveValue> temp = new HashMap<String, PrimitiveValue>();		
		if(this.searchIndexIterator == null || !this.searchIndexIterator.hasNext()) {
			this.nonIndexedTuple = this.nonIndexedIterator.next();
			if(this.nonIndexedTuple != null) {
				try {
					this.searchIndexIterator = this.btree.search(this.nonIndexedTuple.get(this.nonIndexedColumnStr), this.indexedColumnStr);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				return null;
			}
		}
		Map<String, PrimitiveValue> searchedTuple = this.searchIndexIterator.next();
		if(searchedTuple != null) {
			for(String key: searchedTuple.keySet()) {
				temp.put(key, searchedTuple.get(key));
			}
			for(String key: this.nonIndexedTuple.keySet()) {
				temp.put(key, this.nonIndexedTuple.get(key));
			}
			return temp;
		}
		return null;
	}
	
	@Override
	public void reset() {
		this.nonIndexedIterator.reset();
		this.indexedIterator.reset();
	}

	@Override
	public List<String> getColumns() {
		return this.columns;
	}

	@Override
	public DefaultIterator getChildIter() {
		return null;
	}
}