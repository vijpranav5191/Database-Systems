package queryexec;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import interfaces.OnTupleGetListener;
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
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
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
		this.joins = this.plainselect.getJoins();
		FileUtils.getDBContents(this.table.getName(), this);
	}
	
	@Override
	public void onTupleReceived(String tuple, String tableName) {
		String[] row = tuple.split("\\|");
		List<ColumnDefs> cdefs = SchemaStructure.schema.get(tableName);
		Map<String, PrimitiveValue> maps = new HashMap<String, PrimitiveValue>();
		
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
			maps.put(cdef.cdef.getColumnName(), pm);
		}

		try {
			Boolean bool = EvaluateUtils.evaluate(maps, this.whereExp);
			if(bool) {
				System.out.println(tuple);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
