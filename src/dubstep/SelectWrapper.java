package dubstep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SelectWrapper {
	private PlainSelect plainselect;
	private Table table;
	private List<SelectItem> selectItems;
	private Expression whereExp;
	
	public SelectWrapper(PlainSelect plainselect){
		this.plainselect = plainselect;
	}
	
	public void parse() {
		FromItem fromItem = this.plainselect.getFromItem();
		this.selectItems = this.plainselect.getSelectItems();
		if(fromItem instanceof Table) {
			this.table = (Table) fromItem;
		}
		this.whereExp = this.plainselect.getWhere();
		ArrayList<String> data = FileUtils.getDBContents(this.table.getName().toLowerCase());
		List<ColumnDefs> cdefs = SchemaStructure.schema.get(this.table.getName());
	
		HashMap<String,List<PrimitiveValue>> dataMap = new HashMap<String, List<PrimitiveValue>>();
		for(int i = 0;i < data.size();i++) {
			String[] row = data.get(i).split("|");
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
				List<PrimitiveValue> pmList = dataMap.getOrDefault(cdef.getColumnName(), new ArrayList<PrimitiveValue>());
				pmList.add(pm);
				dataMap.put(cdef.getColumnName(), pmList);
			}
		}
		
		
	}
}
