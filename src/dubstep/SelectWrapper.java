package dubstep;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
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
		System.out.print(data);
		
//		String fullname = tbalname+cd.getColumnName();
//		String dt = cd.getColDataType().getDataType().toLowerCase();
//		PrimitiveValue pm = null;
//		switch (dt) {
//			case "int":
//				 pm = new LongValue(0);
//				break;
//			case "string":
//				pm = new StringValue("");
//				break;
//			case "varchar":
//				pm = new StringValue("");
//				break;	
//			case "char":
//				pm = new StringValue("");
//				break;
//			case "decimal":
//				pm = new DoubleValue(0);
//				break;
//			case "date":
//				pm = new DateValue("");
//				break;
//			default:
//				break;
	}
}
