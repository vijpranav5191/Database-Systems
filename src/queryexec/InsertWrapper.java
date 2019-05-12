package queryexec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import utils.ColumnSeparator;
import utils.Config;

public class InsertWrapper {
	Insert insert;
	
	public InsertWrapper(Insert insert){
		this.insert = insert;
	}
	
	public void insertValues(){
		List<Column> columns = this.insert.getColumns();
		List<String> columnStr = new ArrayList<String>();
		for(int i = 0; i < columns.size();i++) {
			columnStr.add(columns.get(i).getColumnName());
		}
		
		ColumnSeparator colSep = new ColumnSeparator(this.insert.getTable(), columnStr, Config.insertTemp);
	
		ExpressionList values = (ExpressionList) this.insert.getItemsList();
		List<Expression> exps = values.getExpressions();
		try {
			colSep.executeSingle(exps);
		} catch (IOException e) {
			e.printStackTrace();
		}
		colSep.close();
	}
}
