package dubstep;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.StringReader;
import interfaces.UnionWrapper;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;
import queryexec.CreateWrapper;
import queryexec.DeleteWrapper;
import queryexec.InsertWrapper;
import queryexec.SelectWrapper;
import utils.Config;
import utils.Utils;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;

class Main {

	public static void main(String args[]) throws Exception {
		Config.isInMemory = true;
		Utils.createDirectory(Config.folderName);
		Utils.createDirectory(Config.createFileDir);		
		Utils.createDirectory(Config.bPlusTreeDir);
		Utils.createDirectory(Config.secIndexdir);
		Utils.createDirectory(Config.columnSeparator);
		Utils.createDirectory(Config.insertTemp);
		
////		File createDir = new File(Config.createFileDir);
////		CreateWrapper createWrapper = new CreateWrapper();
////		for(File file: createDir.listFiles()) {
////			createWrapper.createHandler(file.getName());
////		}
		
		for (String arg : args) {
			if (arg.equals("--in-mem")) {
				Config.isInMemory = true;
			}
		}
		System.out.println("$> "); // print a prompt
		while (true) {
			
//			/* code to parse stdin as a string and then feed to JSQL parser */
//			BufferedInputStream buf = new BufferedInputStream();	
//			ByteArrayOutputStream baos = new ByteArrayOutputStream();
//			int result = buf.read();
//			while(result != 59) {
//				baos.write((byte) result);
//				result = buf.read();
//			}
//			if(baos.size()<=1) {
//				break;
//			}
		//	String querystr = baos.toString();
			CCJSqlParser parser = new CCJSqlParser(System.in);
			Statement query = parser.Statement();
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
				CreateWrapper cw = new CreateWrapper();
				cw.createHandler(query);
			} else if(query  instanceof Insert) {
				Insert insert = (Insert) query;
				InsertWrapper insertWrapper = new InsertWrapper(insert);
				insertWrapper.insertValues();
			}else if(query instanceof Delete) {
				Delete delete = (Delete) query;
				DeleteWrapper deleteWrapper = new DeleteWrapper(delete);
				deleteWrapper.parse();
			}
			System.out.println("$>"); // print a prompt after executing each command
		}
	}
}
