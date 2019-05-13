package queryexec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import objects.SchemaStructure;

public class DeleteWrapper {

	Delete delete;
	public DeleteWrapper(Delete delete) {
		// TODO Auto-generated constructor stub
		this.delete = delete;
	}

	public void parse() {
		// TODO Auto-generated method stub
		Table  table = this.delete.getTable();
		Expression exp = this.delete.getWhere();
		List<Expression> deleteExpList = SchemaStructure.deleteMap.get(table.getName());
		if(deleteExpList == null) {
			SchemaStructure.deleteMap = new HashMap<>();
			List<Expression> temp =  new ArrayList<Expression>();
			temp.add(exp);
			SchemaStructure.deleteMap.put(table.getName(), temp);		
		} else {
			SchemaStructure.deleteMap.get(table.getName()).add(exp);
		}
	}

}
