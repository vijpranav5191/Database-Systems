package queryexec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import objects.ColumnDefs;
import objects.SchemaStructure;

public class GroupByWrapper {
	ArrayList<String> data;
	private List<Column> groupByColumns;
	List<ColumnDefs> cdefs;
	
	public GroupByWrapper(ArrayList<String> data, List<Column> groupByColumns, String tableName){
		this.data = data;
		this.groupByColumns = groupByColumns;
		this.cdefs = SchemaStructure.schema.get(tableName);		
	}
	
	public void parse() {
		ArrayList<ArrayList<String>> groupList = new ArrayList<ArrayList<String>>();
		groupList.add(this.data);
		
		
		
		for(int colIndex = 0; colIndex < this.groupByColumns.size(); colIndex++) {
			Column column = this.groupByColumns.get(colIndex);
			
			int index = 0;
			while(index < this.cdefs.size() && !column.getColumnName().equals(this.cdefs.get(index).cdef.getColumnName())) {
				index += 1;
			}
			ArrayList<ArrayList<String>> tempList = new ArrayList<ArrayList<String>>();
			int groupListIndex = 0;
			
			
			while(groupList.size() > groupListIndex) {
				Map<String, ArrayList<String>> map = groupByColumn(index, groupList.get(groupListIndex));
				for(String key: map.keySet()) {
					tempList.add(map.get(key));
				}
				groupListIndex++;
			}
			groupList = tempList;
		}
	}
	
	
	public Map<String, ArrayList<String>> groupByColumn(int index, ArrayList<String> data){
		Map<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>(); 
		for(int i = 0; i < data.size(); i++) {
			String[] row = data.get(i).split("\\|");
			ArrayList<String> list = map.getOrDefault(row[index], new ArrayList<>());
			list.add(data.get(i));
			map.put(row[index], list);
		}
		return map;
	}
}
