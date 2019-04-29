package bPlusTree;

import java.io.IOException;
import java.util.Map;

import iterators.DefaultIterator;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import utils.Config;
import utils.Utils;

public class BPlusTreeBuilder {
	
	DefaultIterator iterator;
	BPlusTree bPlusTree;
	String indexStr;
	public BPlusTreeBuilder(DefaultIterator iterator){
		this.iterator = iterator;
	}
	
	public BPlusTree build(String indexStr) {
		this.bPlusTree = new BPlusTree(Config.BRANCHING_FACTOR, indexStr);
		this.indexStr = indexStr;
//		int[] indexes = {1,2,2,3,3,3,3,4,4,4,5,5,5,6,6,6,6,6,6,6,6,6,7,7,7,7,7};
//		for(int i=0;i < indexes.length;i++) {
//			LongValue longValue = new LongValue(indexes[i]);
//			this.bPlusTree.insert(longValue, null);
//		}
		while(this.iterator.hasNext()) {
			Map<String, PrimitiveValue> next = this.iterator.next();
			this.bPlusTree.insert(next.get(this.indexStr), next);
		}
		System.out.println("BPlus Tree Done");
		return this.bPlusTree;
	}
	
	public void close() throws IOException {
		if(bPlusTree != null) {
			bPlusTree.close();
		}
	}
}
