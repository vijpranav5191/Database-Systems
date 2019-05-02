package bPlusTree;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import iterators.DefaultIterator;

import iterators.RAIterator;
import iterators.TableSeekIterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.Join;
import objects.ColumnDefs;
import objects.SchemaStructure;
import utils.Config;
import utils.Utils;

public class BPlusTreeBuilder {
	
	RAIterator iterator;
	public BPlusTree bPlusTree;
	String indexStr;
	Table table;
	List<ColumnDefinition> cdefs;
	
	public BPlusTreeBuilder(RAIterator iterator, Table table, List<ColumnDefinition> cdefs){
		this.iterator = iterator;
		this.table = table;
		this.cdefs = cdefs;
	}
	
	public BPlusTree build(String indexStr) {
		this.bPlusTree = new BPlusTree(Config.BRANCHING_FACTOR, indexStr);
		this.indexStr = indexStr;
		int position = getPositionOfColumn(this.indexStr);
		
		String startPoint = "";
		int startOffset = 0;
		int seekOffset = 0;
		int insertedCount = 0;
		if(position >= 0) {
			while(this.iterator.hasNext()) {
				String next = this.iterator.next();
				String[] arr = next.split("\\|");
				LongValue longValue = new LongValue(arr[position]);
				if(!startPoint.equals(arr[position])) {
					startPoint = arr[position];
					startOffset += seekOffset;				
					seekOffset = 0;
				}
				seekOffset += (next.length() + 1); 
				this.bPlusTree.insert(longValue, startOffset);
				insertedCount += 1;
			}
			try {
				this.bPlusTree.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("BPlus Tree Done" + insertedCount);
		return this.bPlusTree;
	}
	
	private int getPositionOfColumn(String indexStr) {
		int index = 0;
		for(ColumnDefinition cdef: this.cdefs) {
			if(cdef.getColumnName().toLowerCase().equals(indexStr.toLowerCase())) {
				return index;
			}
			index++;
		}
		return -1;
	}


	public DefaultIterator search(PrimitiveValue searchValue, String indexColumn) throws IOException {
		if(searchValue == null) {
			bPlusTree.search(searchValue);	
		}
		int position = bPlusTree.search(searchValue);
		BufferedReader br = Utils.getInputStreamBySeek(Config.databasePath + table.getName() + ".csv", position);
		TableSeekIterator tableSeekItr = new TableSeekIterator(br, this.table, searchValue, indexColumn);
		return tableSeekItr;
	}
	
	public void close() throws IOException {
		if(bPlusTree != null) {
			bPlusTree.close();
		}
	}

	

	public static DefaultIterator searchByRange(DefaultIterator iterator2, Table table, Expression exp) {
		// TODO Auto-generated method stub
		return null;
	}
}
