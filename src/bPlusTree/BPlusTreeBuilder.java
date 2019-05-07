package bPlusTree;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import bPlusTree.BPlusTree.LeafNode;
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

public class BPlusTreeBuilder{
	
	private RandomAccessFile raf_1 = null;
	
	RAIterator iterator;
	public BPlusTree bPlusTree;
	String indexStr;
	Table table;
	List<ColumnDefinition> cdefs;
	
	public BPlusTreeBuilder(RAIterator iterator, Table table, List<ColumnDefinition> cdefs, String indexStr){
		this.iterator = iterator;
		this.table = table;
		this.cdefs = cdefs;
		this.indexStr = indexStr;
	}
	
	
	public BPlusTreeBuilder(Table table, List<ColumnDefinition> cdefs, String indexStr){
		this.table = table;
		this.cdefs = cdefs;
		this.indexStr = indexStr;
	}
	
	
	public BPlusTree build() {
		this.bPlusTree = new BPlusTree(Config.BRANCHING_FACTOR, indexStr);
		int position = getPositionOfColumn(this.indexStr);
		
		String startPoint = "";
		int startOffset = 0;
		int seekOffset = 0;
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
			}
			try {
				this.bPlusTree.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
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


	public DefaultIterator search(PrimitiveValue searchValue, String indexColumn, List<String> queryColumns) throws IOException {
		int position = bPlusTree.search(searchValue);
		BufferedReader br = getInputStreamBySeek(Config.databasePath + table.getName() + ".csv", position);
		TableSeekIterator tableSeekItr = new TableSeekIterator(br, this.table, searchValue, indexColumn, queryColumns);
		return tableSeekItr;
	}
	
	public void close() throws IOException {
		if(bPlusTree != null) {
			bPlusTree.close();
		}
	}

	public BufferedReader getInputStreamBySeek(String path, int seekPosition) throws IOException {
		try {
			if(raf_1 == null) {
				raf_1 = new RandomAccessFile(path, "r");
			}
			raf_1.seek(seekPosition);
		} catch (IOException e) {
			e.printStackTrace();
		}
		InputStream is = Channels.newInputStream(raf_1.getChannel());
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		return br;
	}
	
	public void toDraw() throws IOException {
		this.bPlusTree.toDraw(this.table);
	}
	
	public void writeMapToFile() throws IOException {
		List<Integer> values = ((LeafNode)this.bPlusTree.root).values;
		List<PrimitiveValue> keys = this.bPlusTree.root.keys;
		File filename = new File(Config.bPlusTreeDir + this.table + "__" + this.indexStr);
		BufferedWriter writer = new BufferedWriter(new FileWriter(filename));   
		for(int i=0;i < values.size();i++) {
			writer.write(keys.get(i) + "," + values.get(i));
			writer.newLine();
		}
		writer.close();
	}
	
	public void readMapFromFile() {
		List<Integer> values = new ArrayList<>();
		List<PrimitiveValue> keys = new ArrayList<>();
		String line;
		try {
			BufferedReader br = new BufferedReader(new FileReader(Config.bPlusTreeDir + this.table + "__" + this.indexStr));
			while ((line = br.readLine()) != null) {
				String[] l = line.split("\\,");
				keys.add(new LongValue(l[0]));
				values.add(Integer.parseInt(l[1]));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.bPlusTree = new BPlusTree(Config.BRANCHING_FACTOR, indexStr);
		this.bPlusTree.createFromDisk(keys, values);
	}
}
