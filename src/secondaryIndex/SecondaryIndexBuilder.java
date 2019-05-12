package secondaryIndex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;



import bPlusTree.BPlusTree.LeafNode;
import iterators.DefaultIterator;
import iterators.FileReaderIterator;
import iterators.RAIterator;
import iterators.TableSeekBySecIndexIterator;
import iterators.TableSeekIterator;
import iterators.TableSeekSecIndexByRangeIterator;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import utils.Config;

public class SecondaryIndexBuilder {
	RAIterator iterator;
	String indexStr;
	String indexColType;
	Table table;
	private int position;
	private RandomAccessFile raf_1 = null;
	List<ColumnDefinition> cdefs;
	Map<PrimitiveValue, List<Integer>> index;
	public SecondaryIndexBuilder(FileReaderIterator iter, Table tbal, List<ColumnDefinition> cdef, String indexKey) {
		// TODO Auto-generated constructor stub
		this.iterator = iter;
		this.table = tbal;
		this.indexStr = indexKey;
		this.cdefs = cdef;
		this.position = getPositionOfColumn(this.indexStr);
		this.indexColType = this.cdefs.get(position).getColDataType().getDataType();
		this.index = new TreeMap<>(	new Comparator<PrimitiveValue>(){
			@Override
			public int compare(PrimitiveValue o1, PrimitiveValue o2) {
				if(o1 instanceof DateValue){
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
					Date dateFirst;
					try {
						dateFirst = sdf.parse(String.valueOf(o1));
						Date dateSecond = sdf.parse(String.valueOf(o2));
						return  ((dateFirst.compareTo(dateSecond)));
					} catch (ParseException e) {
						e.printStackTrace();
					}	
				}
				return o1.toString().compareTo(o2.toString());
			}
			
		});
//		this.index = new TreeMap<>();
	}
	public  Map<PrimitiveValue, List<Integer>> build() {
//		SecondaryIndex secIndex = new SecondaryIndex(this.indexStr);
		int startOffset = 0;
//		int seekOffset = 0;
		if(this.position >= 0) {
			while(this.iterator.hasNext()) {
				String next = this.iterator.next();
				String[] arr = next.split("\\|");
				PrimitiveValue pm;
				switch (this.indexColType) {
				case "INT":
					pm = new LongValue(arr[position]);
					break;
				case "STRING":
					pm = new StringValue(arr[position]);
					break;
				case "VARCHAR":
					pm = new StringValue(arr[position]);
					break;	
				case "CHAR":
					pm = new StringValue(arr[position]);
					break;
				case "DECIMAL":
					pm = new DoubleValue(arr[position]);
					break;
				case "DATE":
					pm = new DateValue(arr[position]);
					break;
				default:
					pm = new StringValue(arr[position]);
					break;
				}
				if(this.index.get(pm)!=null) {
					this.index.get(pm).add(startOffset);
				}
				else {
					this.index.put(pm, new ArrayList<Integer>(Arrays.asList(startOffset)));
				}
				
//				if(this.index.get(arr[position])!=null) {
//					this.index.get(arr[position]).add(startOffset);
//				}
//				else {
//					this.index.put(arr[position], new ArrayList<Integer>(Arrays.asList(startOffset)));
//				}
				startOffset += (next.length() + 1); 
			}
		}
		return this.index;
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
	public void writeMapToFile() throws IOException {
		File filename = new File(Config.secIndexdir + this.table + "__" + this.indexStr);
		BufferedWriter writer = new BufferedWriter(new FileWriter(filename)); 
		writer.write(this.indexColType+"\n");
		for(PrimitiveValue key : this.index.keySet()) {
			for(Integer i: this.index.get(key)) {
				writer.write(key.toString() + "," + i.toString());
				writer.newLine();
			}
		}
		writer.close();
	}
	public void readMapFromFile() {
		File filename = new File(Config.secIndexdir + this.table + "__" + this.indexStr);
		String line;
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String head = br.readLine();
			PrimitiveValue pm;
			while ((line = br.readLine()) != null) {
				String[] l = line.split("\\,");
				switch (head) {
				case "INT":
					pm = new LongValue(l[0]);
					break;
				case "STRING":
					pm = new StringValue(l[0]);
					break;
				case "VARCHAR":
					pm = new StringValue(l[0]);
					break;	
				case "CHAR":
					pm = new StringValue(l[0]);
					break;
				case "DECIMAL":
					pm = new DoubleValue(l[0]);
					break;
				case "DATE":
					pm = new DateValue(l[0]);
					break;
				default:
					pm = new StringValue(l[0]);
					break;
				}
				if(this.index.get(pm)!=null) {
					this.index.get(pm).add(Integer.parseInt(l[1]));
				}
				else {
					this.index.put(pm, new ArrayList<Integer>(Arrays.asList(Integer.parseInt(l[1]))));
				}
//				if(this.index.get(l[0])!=null) {
//					this.index.get(l[0]).add(Integer.parseInt(l[1]));
//				}
//				else {
//					this.index.put(l[0], new ArrayList<Integer>(Arrays.asList(Integer.parseInt(l[1]))));
//				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public DefaultIterator search(PrimitiveValue key , Expression exp) throws IOException {
		List<Integer> slist = this.index.get(key);
//		BufferedReader br = getInputStreamBySeek(Config.databasePath + this.table.getName() + ".csv", startOffset, endOffset);
		TableSeekBySecIndexIterator tableSeekItr = new TableSeekBySecIndexIterator(slist,this.table, key, this.table.getName()+this.indexStr);
		return tableSeekItr;
	}
	public DefaultIterator search(PrimitiveValue start, PrimitiveValue end, Expression exp){
		Iterator<PrimitiveValue> iterator;
		int loc = binarySearch(this.index.keySet(), start);
		int i = loc >= 0 ? loc : -loc - 1;

		int e = 0;
		if(end!=null) {
			loc = binarySearch(this.index.keySet(), end);
			e = loc >= 0 ? loc : -loc - 1;
//			e = keyset.indexOf(end);
		}
		else {
			iterator = this.index.keySet().iterator();
			while(iterator.hasNext())
				end = iterator.next();
			loc = binarySearch(this.index.keySet(), end);
			e = loc >= 0 ? loc : -loc - 1;
		}
		TableSeekSecIndexByRangeIterator tableseekIter = new TableSeekSecIndexByRangeIterator(this.table, i, e, this.table.getName()+this.indexStr, this.index);
//		TableSeekBySecIndexIteraator tableSeekItr = new TableSeekBySecIndexIterator(slist,this.table, start, this.table.getName()+this.indexStr);
//		keyset = null;
		return tableseekIter;
	}
	public BufferedReader getInputStreamBySeek(String path, int seekPosition, Integer endOffset) throws IOException {
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
	private int binarySearch(Set<PrimitiveValue> kList, PrimitiveValue key) {
		Comparator<PrimitiveValue> comp = new Comparator<PrimitiveValue>(){
			@Override
			public int compare(PrimitiveValue o1, PrimitiveValue o2) {
			    if(o1 instanceof LongValue){
			    	return (int) (((LongValue)o1).toLong() - ((LongValue)o2).toLong());	
				} else if(o1 instanceof DoubleValue){
					return (int) (((DoubleValue)o1).toDouble() - ((DoubleValue)o2).toDouble());
				} else if(o1 instanceof DateValue){
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
					Date dateFirst;
					try {
						dateFirst = sdf.parse(String.valueOf(o1));
						Date dateSecond = sdf.parse(String.valueOf(o2));
						return  ((dateFirst.compareTo(dateSecond)));
					} catch (ParseException e) {
						e.printStackTrace();
					}	
				}
				return o1.toString().compareTo(o2.toString());
			}
			
		};
//		int loc = Collections.binarySearch(kList, key, comp);
		return 0;
	}
}
