package junk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import iterators.DefaultIterator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class OrderByIterator implements DefaultIterator{
	List<OrderByElement> orderbyelements;
	DefaultIterator iterator;
	final public int SortASC = 1;
	final public int SortDESC = -1;
	public int sortDirection = SortASC;
	public ArrayList<ArrayList<Map<String, PrimitiveValue>>> sortedList; 
	
	public OrderByIterator(List<OrderByElement> orderbyelements, DefaultIterator iterator){
		this.orderbyelements = orderbyelements;
		this.iterator = iterator;
		this.sortDirection = SortASC;
		this.sortedList = new ArrayList<ArrayList<Map<String, PrimitiveValue>>>();
		orderDataByElement();
	}

	OrderByIterator(List<OrderByElement> orderbyelements, DefaultIterator iterator, int sortDirection){
		this.orderbyelements = orderbyelements;
		this.iterator = iterator;
		this.sortDirection = sortDirection;
		this.sortedList = new ArrayList<ArrayList<Map<String, PrimitiveValue>>>();
		orderDataByElement();
	}
	
	private void orderDataByElement() {
		ArrayList<Map<String, PrimitiveValue>> list = new ArrayList<Map<String, PrimitiveValue>>();
		while(this.iterator.hasNext()) {
			list.add(this.iterator.next());
		}
		for(OrderByElement orderByElement: this.orderbyelements) {
			for(ArrayList<Map<String, PrimitiveValue>> x :this.sortedList) {
//				ArrayList<>this.sortByCol(x, sortDirection, "");
			}
		}
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		return null;
	}

	@Override
	public void reset() {
		this.iterator.reset();
	}

	@Override
	public List<String> getColumns() {
		return this.iterator.getColumns();
	}

	
	public ArrayList<Map<String, PrimitiveValue>> sortByCol(ArrayList<Map<String, PrimitiveValue>> maps, int sortDirection, String columnName){
		Comparator<Map<String, PrimitiveValue>> comp = new Comparator<Map<String, PrimitiveValue>>(){
			public int compare(Map<String, PrimitiveValue> a, Map<String, PrimitiveValue> b){
				//reverse result if DESC (sortDirection = -1)
				PrimitiveValue aValue = a.get(columnName);
				PrimitiveValue bValue = b.get(columnName);
				
				int value = 0;
				
				return sortDirection * value;
			}
		};
		
		Collections.sort(maps, comp);
		return maps;
	}
}
