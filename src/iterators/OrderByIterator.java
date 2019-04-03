package iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import iterators.DefaultIterator;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import utils.EvaluateUtils;

public class OrderByIterator implements DefaultIterator{
	List<OrderByElement> orderbyelements;
	DefaultIterator iterator;
	final public int SortASC = 1;
	final public int SortDESC = -1;
	int outerPointer = 0; 
	int innerPointer = 0;
	Map<String, PrimitiveValue> nextResult;
	
	public ArrayList<ArrayList<Map<String, PrimitiveValue>>> sortedList; 
	public ArrayList<Map<String, PrimitiveValue>> currentList;
	
	public OrderByIterator(List<OrderByElement> orderbyelements, DefaultIterator iterator){
		this.orderbyelements = orderbyelements;
		this.iterator = iterator;
		this.sortedList = new ArrayList<ArrayList<Map<String, PrimitiveValue>>>();
		this.currentList = new ArrayList<Map<String, PrimitiveValue>>();
		orderDataByElement();
		this.nextResult = this.getNextIter();
	}
	
	private void orderDataByElement() {
		ArrayList<Map<String, PrimitiveValue>> list = new ArrayList<Map<String, PrimitiveValue>>();
		while(this.iterator.hasNext()) {
			list.add(this.iterator.next());
		}
		this.sortedList.add(list);
		
		for(OrderByElement orderByElement: this.orderbyelements) {
			for(ArrayList<Map<String, PrimitiveValue>> x :this.sortedList) {
				if(orderByElement.isAsc()) {
					this.sortByCol(x, 1, orderByElement.getExpression().toString());
				} else {
					this.sortByCol(x, -1, orderByElement.getExpression().toString());	
				}
			}
			this.sortedList = disIntegrateList(orderByElement);
		}
	}

	private ArrayList<ArrayList<Map<String, PrimitiveValue>>> disIntegrateList(OrderByElement orderByElement) {
		String byKey = orderByElement.getExpression().toString();
		ArrayList<ArrayList<Map<String, PrimitiveValue>>> temp = new ArrayList<ArrayList<Map<String, PrimitiveValue>>>();
		
		for(ArrayList<Map<String, PrimitiveValue>> list: this.sortedList) {
			Map<String, PrimitiveValue> curr = null;
			ArrayList<Map<String, PrimitiveValue>>  disIntegratedList = new ArrayList<Map<String, PrimitiveValue>>();
			for(Map<String, PrimitiveValue> element: list) {
				if(curr == null || !curr.get(byKey).equals(element.get(byKey))) {
					curr = element;
					if(disIntegratedList.size() > 0) {
						temp.add(disIntegratedList);
					}
					disIntegratedList = new ArrayList<Map<String, PrimitiveValue>>(); 
					disIntegratedList.add(curr);
				} else {
					disIntegratedList.add(element);
				}
			}
			if(disIntegratedList.size() > 0) {
				temp.add(disIntegratedList);
			}
		}
		return temp;
	}


	@Override
	public boolean hasNext() {
		if(this.nextResult != null) {
			return true;
		}
		return false;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> temp = this.nextResult;
		this.nextResult = getNextIter();
		return temp;
	}


	private Map<String, PrimitiveValue> getNextIter() {
		Map<String, PrimitiveValue> temp = null;
		if(this.innerPointer < this.currentList.size()) {
			temp = this.currentList.get(this.innerPointer);
			this.innerPointer++;
		} else {
			if(this.outerPointer < this.sortedList.size()) {
				this.currentList = this.sortedList.get(this.outerPointer);
				this.outerPointer++;
				this.innerPointer = 0;
				temp = this.getNextIter();
			} 
		}
		return temp;
	}

	@Override
	public void reset() {
		int outerPointer = 0; 
		int innerPointer = 0;
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
				
				Table table = new Table();
				table.setName("R");
				
				Column aCol = new Column();
				aCol.setColumnName("A");
				aCol.setTable(table);
				
				Column bCol = new Column();
				bCol.setColumnName("B");
				bCol.setTable(table);
				
				
				GreaterThan gtt = new GreaterThan();
				gtt.setLeftExpression(aCol);
				gtt.setRightExpression(bCol);
				
				Map<String, PrimitiveValue> scope = new HashMap<String, PrimitiveValue>();
				scope.put(table.getName() + ".A", aValue);
				scope.put(table.getName() + ".B", bValue);
				
				try {
					if(EvaluateUtils.evaluate(scope, gtt)) {
						value = 10;
					} else {
						value = -10;
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return sortDirection * value;
			}
		};
		
		Collections.sort(maps, comp);
		return maps;
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return null;
	}
}