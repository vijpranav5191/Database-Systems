package bPlusTree;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import utils.Config;


public class BPlusTree {
	public Node root;
	private final int DEFAULT_BRANCHING_FACTOR = 128;
	
	private String prevPath;
	File filename;
	BufferedWriter writer;
	int branchingFactor;
	String indexStr;
	
	public BPlusTree(String indexStr){
		this.branchingFactor = DEFAULT_BRANCHING_FACTOR;
		this.root = new LeafNode();
		this.indexStr = indexStr;
	}
	
	public BPlusTree(int branchingFactor, String indexStr){
		this.branchingFactor = branchingFactor;
		this.root = new LeafNode();
		this.indexStr = indexStr;
	}
	
	
	
	public int search(PrimitiveValue key) {
		return root.getValue(key);
	}
	
	public void insert(PrimitiveValue key, int startOffset) {
		//System.out.println("Inserting a value   " + key.toString());
		Node sibling = root.insertValue(key, startOffset);
		
		if(sibling != null) {
			InternalNode newRoot = new InternalNode();
			newRoot.children.add(root);
			newRoot.children.add(sibling);
			newRoot.keys.add(sibling.getFirstLeafKey());
			root = newRoot;
		}
		//toDraw();
	}
	
	public void toDraw(Table table) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(Config.bPlusTreeDir + table.getName() + "_" + this.indexStr));
	    Queue<List<Node>> queue = new LinkedList<List<Node>>();
		queue.add(Arrays.asList(root));
		while (!queue.isEmpty()) {
			Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
			String line = "";
			while (!queue.isEmpty()) {
				List<Node> nodes = queue.remove();
				Iterator<Node> it = nodes.iterator();
				while (it.hasNext()) {
					Node node = it.next();
					if(node instanceof BPlusTree.InternalNode) {
						for(PrimitiveValue x: node.keys) {
							line += x.toString() + ",";
						}
					} else {
						LeafNode node1  = (BPlusTree.LeafNode) node;
						for(PrimitiveValue x: node1.keys) {
							line+= x.toString() + ",";
						}	
					}
					line = line.substring(0, line.length() - 1);
					if (node instanceof BPlusTree.InternalNode) {
						InternalNode internalNode = (InternalNode) node;
						nextQueue.add(internalNode.children);
					}
					line += "|";
				}
			}
			writer.write(line.substring(0, line.length() - 1));
			writer.newLine();
			queue = nextQueue;
		}
	    writer.close();
	}
	
	public void reDraw() {
		
	}
	
	
	public abstract class Node {
		public List<PrimitiveValue> keys;
		
		int keyNumber() {
			return keys.size();
		}
		
		abstract int getValue(PrimitiveValue key);
		
		abstract Node insertValue(PrimitiveValue key,  int value);
		
		abstract PrimitiveValue getFirstLeafKey();
		
		abstract Node split();
		
		abstract boolean isOverflow();
		
	}
	
	
	public class LeafNode extends Node{
		
		LeafNode next;
		public List<Integer> values;
		
		LeafNode() {
			keys = new ArrayList<PrimitiveValue>();
			values = new ArrayList<Integer>();
		}
		
		@Override
		Node insertValue(PrimitiveValue key, int value) {
			int loc = binarySearch(keys, key);
			int valueIndex = loc >= 0 ? loc : -loc - 1;
//			try {
//				writeMapToFile(Config.bPlusTreeDir + indexStr, value);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
			keys.add(valueIndex, key);
			values.add(valueIndex, value);
			if(isOverflow()) {
				return split(); 
			}
			return null;
		}

		@Override
		PrimitiveValue getFirstLeafKey() {
			return keys.get(0);
		}
		
		@Override
		Node split() {
			LeafNode sibling = new LeafNode();
			int from = (keyNumber() + 1) / 2, to = keyNumber();
			sibling.keys.addAll(keys.subList(from, to));
			sibling.values.addAll(values.subList(from, to));
			
			keys.subList(from, to).clear();
			values.subList(from, to).clear();
			
			sibling.next = next;
			next = sibling;
			return sibling;
		}
		
		@Override
		boolean isOverflow() {
			return keys.size() > branchingFactor - 1;
		}

		@Override
		int getValue(PrimitiveValue key) {
			int loc = binarySearch(keys, key);
			return loc >= 0 ? values.get(loc) : -1;
		}
	}
	
	
	public class InternalNode extends Node{
		List<Node> children;

		InternalNode() {
			this.keys = new ArrayList<PrimitiveValue>();
			this.children = new ArrayList<Node>();
		}
		
		@Override
		Node insertValue(PrimitiveValue key, int value) {
			Node child = getChild(key);
			Node sibling = child.insertValue(key, value);
			if(sibling != null) {
				insertChild(sibling.getFirstLeafKey(), sibling);
			}
			if(isOverflow()) {
				return split();
			}
			return null;
		}
		
		void insertChild(PrimitiveValue key, Node child) {
			int loc = binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			keys.add(childIndex, key);
			children.add(childIndex+1 , child);
		}
		
		@Override
		PrimitiveValue getFirstLeafKey() {
			return children.get(0).getFirstLeafKey();
		}
		
		@Override
		Node split() {
			int from = keyNumber() / 2 + 1, to = keyNumber();
			InternalNode sibling = new InternalNode();
			sibling.keys.addAll(keys.subList(from, to));
			sibling.children.addAll(children.subList(from, to + 1));

			keys.subList(from - 1, to).clear();
			children.subList(from, to + 1).clear();

			return sibling;
		}

		@Override
		boolean isOverflow() {
			return children.size() > branchingFactor;
		}
		
		@Override
		int getValue(PrimitiveValue key) {
			return getChild(key).getValue(key);
		}
		
		Node getChild(PrimitiveValue key) {
			int loc = binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			return children.get(childIndex);
		}
	}
	
	
	private int binarySearch(List<PrimitiveValue> kList, PrimitiveValue key) {
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
		int loc = Collections.binarySearch(kList, key, comp);
		return loc;
	}
	String bulkInsert = "";
	public void writeMapToFile(String path, int map) throws IOException {
		if(prevPath != null && prevPath.equals(path) && writer != null) {
			//writer.write(map.toString());
			//writer.newLine();
			this.bulkInsert += (map + "\n");
		} else {
			prevPath = path;
			if(writer != null) {
				if(this.bulkInsert != null) {
					writer.write(this.bulkInsert);
				}
				writer.close();
				this.bulkInsert = "";
			}
			filename = new File(path);
		    writer = new BufferedWriter(new FileWriter(filename));   
		    this.bulkInsert += (map + "\n");
		}
	}
	
	public void close() throws IOException {
		if(writer != null) {
			if(this.bulkInsert != null) {
				writer.write(this.bulkInsert);
			}
			writer.close();
		}
	}

	public void createFromDisk(List<PrimitiveValue> keys, List<Integer> values) {
		this.root.keys = keys;
		((LeafNode)this.root).values = values;
	}
}
