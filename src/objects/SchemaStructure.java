package objects;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Table;

public class SchemaStructure {
	public static HashMap<String, List<ColumnDefs>> schema = new HashMap<>();
	public static HashMap<String, Table> tableMap = new HashMap<>();
	public static HashMap<String, Table> columnTableMap = new HashMap<>();
}