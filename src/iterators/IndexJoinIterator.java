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
	private DefaultIterator leftIterator;
	private DefaultIterator rightIterator;
	private Join join;
	private BPlusTreeBuilder btree;
	private List<String> columns;
	Column indexedColumn, nonIndexedColumn;
	private Map<String, PrimitiveValue> leftTuple;
	public IndexJoinIterator(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join,
			Column indexedColumn, Column nonIndexedColumn) {
		this.leftIterator = leftIterator;
		this.columns = new ArrayList<String>();
		this.join = join;
		if(this.columns.size() == 0) {
			this.columns.addAll(leftIterator.getColumns());
			this.columns.addAll(rightIterator.getColumns());
		}
		this.indexedColumn = indexedColumn;
		this.nonIndexedColumn = nonIndexedColumn;
		this.leftTuple=null;
		this.columns = new ArrayList<String>();
		this.join = join;
		if(this.columns.size() == 0) {
			this.columns.addAll(leftIterator.getColumns());
			this.columns.addAll(rightIterator.getColumns());
		}
		List<ColumnDefinition> cdefs = new ArrayList<ColumnDefinition>();
		for(ColumnDefs cdef: SchemaStructure.schema.get(indexedColumn.getTable().getName())) {
			cdefs.add(cdef.cdef);
		}
		this.btree = SchemaStructure.bTreeMap.get(indexedColumn.getTable().getName());
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
			if(this.rightIterator == null || !this.rightIterator.hasNext()) {
				this.leftTuple = leftIterator.next();
				try {
					String nonIndexedColumn =  this.nonIndexedColumn.getTable().getName() + "." + this.nonIndexedColumn.getColumnName();
					String indexedColumn =  this.indexedColumn.getTable().getName() + "." + this.indexedColumn.getColumnName();
					this.rightIterator = this.btree.search(this.leftTuple.get(nonIndexedColumn), indexedColumn);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}else {
			return null;
		}
		Map<String, PrimitiveValue> rightTuple = this.rightIterator.next();
		if(rightTuple==null) {
			return null;
		}
		for(String key: rightTuple.keySet()) {
			temp.put(key, rightTuple.get(key));
		}
		for(String key: this.leftTuple.keySet()) {
			temp.put(key, this.leftTuple.get(key));
		}	

		return temp;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.leftIterator.reset();
	}

	@Override
	public List<String> getColumns() {
		return this.columns;
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return null;
	}

}