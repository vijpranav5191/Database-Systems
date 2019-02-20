package queryexec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class OrderByWrapper {
	ArrayList<String> data;
	final public int SortASC = 1;
	final public int SortDESC = -1;
	private int sortDirection = SortASC; //1 for ASC, -1 for DESC	
	private List<String[]> records;
	
	public OrderByWrapper(ArrayList<String> data){
		this.data = data;
		this.records = new ArrayList<String[]>();
		for(int i = 0; i < data.size(); i++) {
			String[] row = data.get(i).split("\\|");
			this.records.add(row);
		}
	}
	
	public void setSortDirection(int direction){
		this.sortDirection = direction;
	}
	
	public void sortByCol(final int i){
		
		Comparator<String[]> comp = new Comparator<String[]>(){
			public int compare(String[] a, String[] b){
				//reverse result if DESC (sortDirection = -1)
				return sortDirection * a[i].compareTo(b[i]);
			}
		};
		
		Collections.sort(records, comp);
	}
}
