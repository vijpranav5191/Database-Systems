package dubstep;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class CreateWrapper {

	public void createHandler(Statement query) {
		CreateTable createtab = (CreateTable) query;
		Table tbal = createtab.getTable();
		List<ColumnDefinition> cdef = createtab.getColumnDefinitions();
		List<Index> indexList = createtab.getIndexes();
		Schema.schema.put(tbal, cdef);
	}
}
