package queryexec;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
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
			SchemaStructure.columnTableMap.put(cd.getColumnName(), tbal);		
		}
		SchemaStructure.schema.put(tbal.getName(), cdfList);
		SchemaStructure.tableMap.put(tbal.getName(), tbal);
	}
}
