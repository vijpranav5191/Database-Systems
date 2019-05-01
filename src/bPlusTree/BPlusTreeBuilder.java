package bPlusTree;

import java.util.Map;

import iterators.DefaultIterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;

public class BPlusTreeBuilder {
	
	DefaultIterator iterator;
	BPlusTree bPlusTree;
	String indexStr;
	public BPlusTreeBuilder(DefaultIterator iterator){
		this.iterator = iterator;
	}
	
	public BPlusTree build(String indexStr) {
		this.bPlusTree = new BPlusTree(4);
		this.indexStr = indexStr;
		while(this.iterator.hasNext()) {
			Map<String, PrimitiveValue> next = this.iterator.next();
			this.bPlusTree.insert(next.get(this.indexStr), next);
		}
		System.out.print("BPlus Tree Done");
		return this.bPlusTree;
	}

	public DefaultIterator search(Map<String, PrimitiveValue> lefttuple, Join join) {
		// TODO Auto-generated method stub
		return null;
	}

	

	public static DefaultIterator searchByRange(DefaultIterator iterator2, Table table, Expression exp) {
		// TODO Auto-generated method stub
		return null;
	}
}
