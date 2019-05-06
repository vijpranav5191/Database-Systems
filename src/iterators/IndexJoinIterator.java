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
	Map<String, Integer> columnMapper;
	Map<String, Integer> indexedColumnMapper;
	Map<String, Integer> nonIndexedColumnMapper;
	
	Column indexedColumn, nonIndexedColumn;
	String indexedColumnStr, nonIndexedColumnStr;
	private List<PrimitiveValue> nonIndexedTuple;
	List<PrimitiveValue> nextResult;
	private Map<String, List<String>> queryColumnsMap;
	List<String> queryColumns;
	
	public IndexJoinIterator(DefaultIterator nonIndexedIterator, DefaultIterator indexedIterator, 
			Join join, Column nonIndexedColumn, Column indexedColumn, Map<String, List<String>> queryColumnsMap) {
		this.indexedIterator = indexedIterator;
		this.queryColumnsMap = queryColumnsMap;
		this.queryColumns = this.queryColumnsMap.get(indexedColumn.getTable().getName());
		this.nonIndexedIterator = nonIndexedIterator;
		this.columns = new ArrayList<String>();
		this.join = join;
		this.columns.addAll(nonIndexedIterator.getColumns());
		this.columns.addAll(indexedIterator.getColumns());
		createMapperColumn();
		this.indexedColumn = indexedColumn;
		this.nonIndexedColumn = nonIndexedColumn;
		
		this.btree = SchemaStructure.bTreeMap.get(indexedColumn.getTable().getName());
		this.nonIndexedColumnStr =  this.nonIndexedColumn.getTable().getName() + "." + this.nonIndexedColumn.getColumnName();
		this.indexedColumnStr =  this.indexedColumn.getTable().getName() + "." + this.indexedColumn.getColumnName();	
		this.nextResult = getNextIter();
	}

	private void createMapperColumn() {
		this.columnMapper = new HashMap<String, Integer>();
		int index = 0;
		for(String col: this.columns) {
			this.columnMapper.put(col, index);
			index += 1;
		}
		this.indexedColumnMapper = new HashMap<String, Integer>();
		index = 0;
		for(String col: this.indexedIterator.getColumns()) {
			this.indexedColumnMapper.put(col, index);
			index+=1;
		}
		
		this.nonIndexedColumnMapper = new HashMap<String, Integer>();
		index = 0;
		for(String col: this.nonIndexedIterator.getColumns()) {
			this.nonIndexedColumnMapper.put(col, index);
			index+=1;
		}	
	}
	
	@Override
	public boolean hasNext() {
		if(this.nextResult != null) {
			return true;
		}
		return false;
	}

	@Override
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> temp = this.nextResult;
		this.nextResult = getNextIter();
		return temp;
	}


	public List<PrimitiveValue> getNextIter(){
		List<PrimitiveValue> temp = new ArrayList<PrimitiveValue>();		
		if(this.searchIndexIterator == null || !this.searchIndexIterator.hasNext()) {
			this.nonIndexedTuple = this.nonIndexedIterator.next();
			if(this.nonIndexedTuple != null) {
				try {
					this.searchIndexIterator = this.btree.search(this.nonIndexedTuple
									.get(this.columnMapper.get(this.nonIndexedColumnStr)), this.indexedColumnStr, this.queryColumns);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				return null;
			}
		}
		List<PrimitiveValue> searchedTuple = this.searchIndexIterator.next();
		if(searchedTuple != null) {
			temp.addAll(this.nonIndexedTuple);
			temp.addAll(searchedTuple);
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
}