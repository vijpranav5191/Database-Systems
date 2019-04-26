package dubstep;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

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
import sun.misc.IOUtils;
import sun.nio.ch.IOUtil;
import utils.Config;
import net.sf.jsqlparser.statement.create.table.CreateTable;

class Main{
	
	public static void main(String args[]) throws Exception {
		Config.isInMemory = false;
		File f = new File(Config.fileName); 
		File create = new File(Config.createfiles);
		create.mkdirs();
		f.mkdirs();
		for (String arg : args) {
			if (arg.equals("--in-mem")){
				Config.isInMemory = true;
	        }
		}
		System.out.println("$> "); // print a prompt
		String querystr;
		CreateWrapper cw = new CreateWrapper();
		while(true){
			Scanner br = new Scanner(System.in).useDelimiter("\r\n");
			StringBuilder sb = new StringBuilder();
            while (br.hasNext()) {
            	sb = sb.append(br.next());
            }
            if(sb.toString().equals("\r")) {
            	break;
            }
			StringReader str = new StringReader(sb.toString());
			CCJSqlParser parser = new CCJSqlParser(str);
			Statement query = parser.Statement();
			if(query instanceof Select) 
				{
			    Select select = (Select) query;

			    SelectBody selectbody = select.getSelectBody();
			    
			    if(selectbody instanceof PlainSelect) {
			    	PlainSelect plainSelect = (PlainSelect) selectbody;
			    	new SelectWrapper(plainSelect).parse();			
			    }

			    else {
			    	Union union = (Union) selectbody;
			    	new UnionWrapper(union).parse();
			    }
			} else if(query instanceof CreateTable) {
//				cw.createHandler(query);
				cw.saveCreateStructure(query,sb.toString());
			}
			
			
			System.out.println("$>"); // print a prompt after executing each command
		}
	}
}

