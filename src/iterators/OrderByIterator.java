package iterators;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import iterators.DefaultIterator;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
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
	
	List<String> coulmnsForExternal;
	List<PrimitiveValue> nextResult;
	List<String> columns;
	
	Map<String, Integer> columnMapper;
	public ArrayList<ArrayList<List<PrimitiveValue>>> sortedList; 
	public ArrayList<List<PrimitiveValue>> currentList;
	
	

	public OrderByIterator(List<OrderByElement> orderbyelements, DefaultIterator iterator){
		this.orderbyelements = orderbyelements;
		this.iterator = iterator;
		this.columns = this.iterator.getColumns();
		this.sortedList = new ArrayList<ArrayList<List<PrimitiveValue>>>();
		this.currentList = new ArrayList<List<PrimitiveValue>>();
		createMapperColumn();
		
		
		ArrayList<List<PrimitiveValue>> list = new ArrayList<>();
		while(this.iterator.hasNext()) {
			list.add(this.iterator.next());
		}
		this.sortedList.add(list);
		orderDataByElement();
		this.nextResult = this.getNextIter();
	}
	
	public OrderByIterator(List<OrderByElement> orderbyelements, List<Map<String, PrimitiveValue>> iterator, List<String> coulmnsForExternal){
		this.orderbyelements = orderbyelements;
		this.coulmnsForExternal = coulmnsForExternal;
		this.columns = this.iterator.getColumns();
		this.sortedList = new ArrayList<ArrayList<List<PrimitiveValue>>>();
		this.currentList = new ArrayList<List<PrimitiveValue>>();
		createMapperColumn();
		
		ArrayList<List<PrimitiveValue>> list = new ArrayList<>();
		while(this.iterator.hasNext()) {
			list.add(this.iterator.next());
		}
		this.sortedList.add(list);
		orderDataByElement();
		this.nextResult = this.getNextIter();
	}
	
	private void createMapperColumn() {
		this.columnMapper = new HashMap<String, Integer>();
		int index = 0;
		for(String col: this.columns) {
			this.columnMapper.put(col, index);
			index+=1;
		}
	}
	private void orderDataByElement() {
		for(OrderByElement orderByElement: this.orderbyelements) {
			for(ArrayList<List<PrimitiveValue>> x :this.sortedList) {
				if(orderByElement.isAsc()) {
					Column col= (Column) orderByElement.getExpression();
					if(col.getTable() != null) {
						this.sortByCol(x, SortASC, col.toString());	
					} else {
						this.sortByCol(x, SortASC, col.getColumnName());		
					}
				} else {
					Column col= (Column) orderByElement.getExpression();
					if(col.getTable() != null) {
						this.sortByCol(x, SortDESC, col.toString());	
					} else {
						this.sortByCol(x, SortDESC, col.getColumnName());		
					}
				}
			}
			this.sortedList = disIntegrateList(orderByElement);
		}
	}

	private ArrayList<ArrayList<List<PrimitiveValue>>> disIntegrateList(OrderByElement orderByElement) {
		String byKey = null;
		Column col= (Column) orderByElement.getExpression();
		if(col.getTable() != null) {
			byKey = col.toString();	
		} else {
			byKey = col.getColumnName();		
		}
		ArrayList<ArrayList<List<PrimitiveValue>>> temp = new ArrayList<ArrayList<List<PrimitiveValue>>>();
		
		for(ArrayList<List<PrimitiveValue>> list: this.sortedList) {
			List<PrimitiveValue> curr = null;
			ArrayList<List<PrimitiveValue>>  disIntegratedList = new ArrayList<List<PrimitiveValue>>();
			for(List<PrimitiveValue> element: list) {
				if(curr == null || !curr.get(this.columnMapper.get(byKey)).equals(element.get(this.columnMapper.get(byKey)))) {
					curr = element;
					if(disIntegratedList.size() > 0) {
						temp.add(disIntegratedList);
					}
					disIntegratedList = new ArrayList<List<PrimitiveValue>>(); 
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
	public List<PrimitiveValue> next() {
		List<PrimitiveValue> temp = this.nextResult;
		this.nextResult = getNextIter();
		return temp;
	}


	private List<PrimitiveValue> getNextIter() {
		List<PrimitiveValue> temp = null;
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
		this.outerPointer = 0; 
		this.innerPointer = 0;
	}

	@Override
	public List<String> getColumns() {
		if(this.iterator != null) {
			return this.iterator.getColumns();
		}
		return coulmnsForExternal;
	}

	
	public ArrayList<List<PrimitiveValue>> sortByCol(ArrayList<List<PrimitiveValue>> maps, int sortDirection, String columnName){
		Comparator<List<PrimitiveValue>> comp = new Comparator<List<PrimitiveValue>>(){
			public int compare(List<PrimitiveValue> a, List<PrimitiveValue> b){
				//reverse result if DESC (sortDirection = -1)
				PrimitiveValue aValue = a.get(columnMapper.get(columnName));
				PrimitiveValue bValue = b.get(columnMapper.get(columnName));
				List<PrimitiveValue> scope = new ArrayList<PrimitiveValue>();
				Map<String, Integer> mapper = new HashMap<>();
				scope.add(aValue);
				scope.add(bValue);
				
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
				
				mapper.put(aCol.toString(), 0);
				mapper.put(bCol.toString(), 1);
				
				try {
					if(aValue instanceof StringValue) {
						return aValue.toString().compareTo(bValue.toString()) * sortDirection;

					} 
				
					else if(aValue instanceof DoubleValue){
						return (int) ((aValue.toDouble() - bValue.toDouble()) * sortDirection);
					}
					else if(aValue instanceof LongValue)
					{
						return (int) ((aValue.toLong() - bValue.toLong()) * sortDirection);	
					}
					else if(aValue instanceof DateValue){
						
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
						Date dateFirst = sdf.parse( String.valueOf(aValue) );
						Date dateSecond = sdf.parse(String.valueOf(bValue));
					
						
						return  ((dateFirst.compareTo(dateSecond)) * sortDirection);	
					}
					else {

						if(EvaluateUtils.evaluate(scope, gtt, mapper)) {
							value = 10;
						} else {
							value = -10;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return sortDirection * value;
			}
		};
		
		Collections.sort(maps, comp);
		return maps;
	}
}