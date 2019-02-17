package dubstep;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class CreateWrapper {

	public void createHandler(Statement query) {
		CreateTable createtab = (CreateTable) query;
		Table tbal = createtab.getTable();
		String tbalname = tbal.getName();
		List<ColumnDefinition> cdef = createtab.getColumnDefinitions();
		List<Index> indexList = createtab.getIndexes();
		for (ColumnDefinition cd : cdef) {
			String fullname = tbalname+cd.getColumnName();
			String dt = cd.getColDataType().getDataType().toLowerCase();
			PrimitiveValue pm = null;
			switch (dt) {
			case "int":
				 pm = new LongValue(0);
				break;
			case "string":
				pm = new StringValue("");
				break;
			case "varchar":
				pm = new StringValue("");
				break;	
			case "char":
				pm = new StringValue("");
				break;
			case "decimal":
				pm = new DoubleValue(0);
				break;
			case "date":
				pm = new DateValue("");
				break;
			default:
				break;
			}
			Schema.tupleMap.put(fullname, pm);
		}
		Schema.schema.put(tbal, cdef);
		}
}

class Evaluate extends Eval{
 
		public PrimitiveValue eval(Column col){
			String name = col.getColumnName();
			if(col.getTable() != null && col.getTable().getName() != null){
//				name = col.getTable().getName() + "." + col
				List<ColumnDefinition> cd = Schema.schema.get(col.getTable());
			}
			return null;

//			return scope.get(name);
}
}