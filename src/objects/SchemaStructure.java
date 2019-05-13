package objects;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import bPlusTree.BPlusTreeBuilder;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.Index;
import secondaryIndex.SecondaryIndexBuilder;

public class SchemaStructure {
	public static HashMap<String, List<ColumnDefs>> schema = new HashMap<>();
	public static HashMap<String, Table> tableMap = new HashMap<>();
	public static HashMap<String, Table> columnTableMap = new HashMap<>();
	public static List<Expression> whrexpressions = new ArrayList<Expression>();
	public static HashMap<String, List<Index>> indexMap = new HashMap<>();
	public static HashMap<String, BPlusTreeBuilder> bTreeMap = new HashMap<>();
	public static HashMap<String, Integer> precedenceMap = new HashMap<>();
	public static HashMap<String, SecondaryIndexBuilder> secIndexMap = new HashMap<>();
	public static HashMap <String, List<Expression>> deleteMap = new HashMap<>();
}