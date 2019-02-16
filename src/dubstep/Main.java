package dubstep;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

class Main{
	public static void main(String args[]) throws SQLException, ParseException {
		CCJSqlParser parser = new CCJSqlParser(System.in);
		System.out.println("$> "); // print a prompt
		Statement query;
		CreateWrapper cw = new CreateWrapper();
		while((query = parser.Statement()) != null){
			if(query instanceof Select) {
			    Select select = (Select) query;
			    SelectBody selectbody = select.getSelectBody();
			    if(selectbody instanceof PlainSelect) {
			    	PlainSelect plainSelect = (PlainSelect) selectbody;
			    	new SelectWrapper(plainSelect).parse();			
			    }
			} else if(query instanceof CreateTable) {
				cw.createHandler(query);
				CreateTable createtab = (CreateTable) query;
				Table tbal = createtab.getTable();
				List<ColumnDefinition> lcd = Schema.schema.get(tbal);
				for (ColumnDefinition columnDefinition : lcd) {
					System.out.println(columnDefinition);
				}		
			}
			System.out.println("$> "); // print a prompt after executing each command
		}
	  

	}
}

class Schema{
	static HashMap<Table, List<ColumnDefinition>> schema = new HashMap<>();
}