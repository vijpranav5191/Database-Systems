package dubstep;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import bPlusTree.BPlusTreeBuilder;
import interfaces.UnionWrapper;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;
import queryexec.CreateWrapper;
import queryexec.SelectWrapper;
import utils.Config;
import utils.Utils;
import net.sf.jsqlparser.statement.create.table.CreateTable;

class Main {

	public static void main(String args[]) throws Exception {
		Utils.createDirectory(Config.folderName);
		Utils.createDirectory(Config.createFileDir);		
		Utils.createDirectory(Config.bPlusTreeDir);
//		BPlusTreeBuilder bPlusTreeBuilder = new BPlusTreeBuilder(null);
//		bPlusTreeBuilder.build("LINEITEM.ORDERKEY");

		Config.isInMemory = true;
		for (String arg : args) {
			if (arg.equals("--in-mem")) {
				Config.isInMemory = true;
			}
		}
		CCJSqlParser parser = new CCJSqlParser(System.in);
		System.out.println("$> "); // print a prompt

		Statement query;
		CreateWrapper cw = new CreateWrapper();
		while ((query = parser.Statement()) != null) {
			if (query instanceof Select) {
				Select select = (Select) query;
				SelectBody selectbody = select.getSelectBody();
				if (selectbody instanceof PlainSelect) {
					PlainSelect plainSelect = (PlainSelect) selectbody;
					new SelectWrapper(plainSelect).parse();
				} else {
					Union union = (Union) selectbody;
					new UnionWrapper(union).parse();
				}
			} else if (query instanceof CreateTable) {
				cw.createHandler(query);
			}
			System.out.println("$>"); // print a prompt after executing each command
		}
	}
}
