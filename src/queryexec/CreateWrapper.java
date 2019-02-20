package queryexec;

import java.util.ArrayList;
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
import objects.ColumnDefs;
import objects.SchemaStructure;

public class CreateWrapper {

	public void createHandler(Statement query) {
		CreateTable createtab = (CreateTable) query;
		Table tbal = createtab.getTable();
		
		List<ColumnDefinition> cdef = createtab.getColumnDefinitions();
		List<ColumnDefs> cdfList = new ArrayList<ColumnDefs>();
		
		for (ColumnDefinition cd : cdef) {
			ColumnDefs c = new ColumnDefs();
			c.cdef = cd;
			cdfList.add(c);
		}
		SchemaStructure.schema.put(tbal.getName(), cdfList);
		SchemaStructure.tableMap.put(tbal.getName(), tbal);
	}
}
