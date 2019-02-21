package queryexec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import interfaces.OnTupleGetListener;
import iterators.DefaultIterator;
import iterators.NlJoiniterator;
import iterators.TableScanIterator;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import objects.ColumnDefs;
import objects.SchemaStructure;
import utils.EvaluateUtils;
import utils.FileUtils;

public class SelectWrapper implements OnTupleGetListener {
	private PlainSelect plainselect;
	private Table table;
	private List<SelectItem> selectItems;
	
	private Expression whereExp;
	private List<Join> joins;
	private List<Column> groupByColumns;
	
	public SelectWrapper(PlainSelect plainselect){
		this.plainselect = plainselect;
	}
	
	public void parse() {
		DefaultIterator iter = null;
	
		FromItem fromItem = this.plainselect.getFromItem();
		this.selectItems = this.plainselect.getSelectItems();
		if(fromItem instanceof Table) {
			Table table = (Table) fromItem;
			iter = new TableScanIterator(table);
			
		}
		if((this.joins=this.plainselect.getJoins())!=null){
			for (Join join : joins) {
					iter =  new NlJoiniterator(iter, join);
			}
		}
//		this.whereExp = this.plainselect.getWhere();

//		this.groupByColumns = this.plainselect.getGroupByColumnReferences();
//		FileUtils.getDBContents(this.table.getName(), this);
	}
	
	@Override
	public void onTupleReceived(String tuple, String tableName) {
		Map<String, PrimitiveValue> map = new HashMap<String, PrimitiveValue>();
		map = getPrimitiveMap(tuple, tableName, map);
		if(this.joins != null) {
			onJoin(1, map);
		} else {
			evaluate(map);
		}
	}
	
	public void onJoin(int index,  Map<String, PrimitiveValue> map) {
		//System.out.println("===================");
		if(this.joins != null && this.joins.size() >= index) {
			Join join = this.joins.get(index - 1);
			FromItem fromItem = join.getRightItem();
			if(fromItem instanceof Table) {
				Table table = (Table) fromItem;
				FileUtils.getDBContents(table.getName(), new OnTupleGetListener() {

					@Override
					public void onTupleReceived(String tuple, String tableName) {
						Map<String, PrimitiveValue> map1 = getPrimitiveMap(tuple, tableName, map);
						if(index == joins.size()) {
							evaluate(map1);
						} else {
							onJoin(index + 1, getPrimitiveMap(tuple, tableName, map1));
						}
					}
				});
			}
		}
		//System.out.println("===================");
	}
	
	public Map<String, PrimitiveValue> getPrimitiveMap(String tuple, String tableName, final Map<String, PrimitiveValue> map) {
		String[] row = tuple.split("\\|");
		List<ColumnDefs> cdefs = SchemaStructure.schema.get(tableName);
		for(int j = 0;j < row.length; j++) {
			ColumnDefs cdef = cdefs.get(j);
			String value = row[j];
			PrimitiveValue pm;
			switch (cdef.cdef.getColDataType().getDataType()) {
				case "int":
					 pm = new LongValue(value);
					break;
				case "string":
					pm = new StringValue(value);
					break;
				case "varchar":
					pm = new StringValue(value);
					break;	
				case "char":
					pm = new StringValue(value);
					break;
				case "decimal":
					pm = new DoubleValue(value);
					break;
				case "date":
					pm = new DateValue(value);
					break;
				default:
					pm = new StringValue(value);
					break;
			}
			map.put(tableName + "." + cdef.cdef.getColumnName(), pm);
			//System.out.println(tableName + "." + cdef.cdef.getColumnName() + ":" + pm);
		}
		return map;
	}
	
	public void evaluate(Map<String, PrimitiveValue> map) {
		if(this.whereExp != null) {
			try {
				Boolean bool = EvaluateUtils.evaluate(map, this.whereExp);
				if(bool) {
					System.out.println(map.toString());
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println(map.toString());
		}
	}
}





