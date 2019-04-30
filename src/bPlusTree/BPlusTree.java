package bPlusTree;

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


public class BPlusTree {
	private Node root;
	private final int DEFAULT_BRANCHING_FACTOR = 128;
	
	int branchingFactor;
	
	public BPlusTree(){
		this.branchingFactor = DEFAULT_BRANCHING_FACTOR;
		this.root = new LeafNode();
	}
	
	public BPlusTree(int branchingFactor){
		this.branchingFactor = branchingFactor;
		this.root = new LeafNode();
	}
	
	public void insert(PrimitiveValue key, Map<String, PrimitiveValue> value) {
		//System.out.println("Inserting a value   " + key.toString());
		Node sibling = root.insertValue(key, value);
		
		if(sibling != null) {
			InternalNode newRoot = new InternalNode();
			newRoot.children.add(root);
			newRoot.children.add(sibling);
			newRoot.keys.add(sibling.getFirstLeafKey());
			root = newRoot;
		}
		//toDraw();
	}
	
	public void toDraw() {
		Queue<List<Node>> queue = new LinkedList<List<Node>>();
		queue.add(Arrays.asList(root));
		while (!queue.isEmpty()) {
			Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
			while (!queue.isEmpty()) {
				List<Node> nodes = queue.remove();
				Iterator<Node> it = nodes.iterator();
				while (it.hasNext()) {
					Node node = it.next();
					if(node instanceof BPlusTree.InternalNode) {
						for(PrimitiveValue x: node.keys) {
							System.out.print(x.toString() + " , ");
						}
					} else {
						LeafNode node1  = (BPlusTree.LeafNode) node;
						for(Map<String, PrimitiveValue> x: node1.values) {
							System.out.print(x.toString() + " , ");
						}	
					}
					if (node instanceof BPlusTree.InternalNode) {
						InternalNode internalNode = (InternalNode) node;
						nextQueue.add(internalNode.children);
					}
					System.out.print("---->");
				}
			}
			queue = nextQueue;
			System.out.println("\n");
		}
		System.out.println("==========================================");
	
	}
	
	public Map<String, PrimitiveValue> search(PrimitiveValue key) {
		return root.getValue(key);
	}
	
	abstract class Node {
		List<PrimitiveValue> keys;
		
		int keyNumber() {
			return keys.size();
		}
		
		abstract Map<String, PrimitiveValue> getValue(PrimitiveValue key);
		
		abstract Node insertValue(PrimitiveValue key,  Map<String, PrimitiveValue> value);
		
		abstract PrimitiveValue getFirstLeafKey();
		
		abstract Node split();
		
		abstract boolean isOverflow();
		
	}
	
	
	class LeafNode extends Node{
		
		List<Map<String, PrimitiveValue>> values;
		LeafNode next;

		LeafNode() {
			keys = new ArrayList<PrimitiveValue>();
			values = new ArrayList<Map<String, PrimitiveValue>>();
		}
		
		@Override
		Node insertValue(PrimitiveValue key, Map<String, PrimitiveValue> value) {
			int loc = binarySearch(keys, key);
			int valueIndex = loc >= 0 ? loc : -loc - 1;
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
			return values.size() > branchingFactor - 1;
		}

		@Override
		Map<String, PrimitiveValue> getValue(PrimitiveValue key) {
			return null;
		}
		
	}
	
	
	class InternalNode extends Node{
		List<Node> children;

		InternalNode() {
			this.keys = new ArrayList<PrimitiveValue>();
			this.children = new ArrayList<Node>();
		}
		
		@Override
		Node insertValue(PrimitiveValue key, Map<String, PrimitiveValue> value) {
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
		Map<String, PrimitiveValue> getValue(PrimitiveValue key) {
			return null;
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
	
}
