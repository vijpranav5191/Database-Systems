package queryexec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import objects.SchemaStructure;
import utils.Utils;

public class DeleteWrapper {

	Delete delete;
	public DeleteWrapper(Delete delete) {
		// TODO Auto-generated constructor stub
		this.delete = delete;
	}

	public void parse() {
		Table  table = this.delete.getTable();
		Expression exp = this.delete.getWhere();
		Collection<Expression> expList = Utils.splitAllClauses(exp);
		for(Expression e: expList) {
			if(e instanceof EqualsTo) {
				EqualsTo eq = (EqualsTo) e;
				Expression col = ((EqualsTo) e).getLeftExpression();
				if(col instanceof Column) {
					Column col2 = (Column) col;
					Table tab = col2.getTable();
					if(col2.getTable().getName()==null) {
						col2.setTable(table);
					}
				}
			}
			else if(e instanceof GreaterThan) {
				GreaterThan eq = (GreaterThan) e;
				Expression col = ((GreaterThan) e).getLeftExpression();
				if(col instanceof Column) {
					Column col2 = (Column) col;
					if(col2.getTable().getName()==null) {
						col2.setTable(table);
					}
				}
			}
			else if(e instanceof GreaterThanEquals) {
				GreaterThanEquals eq = (GreaterThanEquals) e;
				Expression col = ((GreaterThanEquals) e).getLeftExpression();
				if(col instanceof Column) {
					Column col2 = (Column) col;
					if(col2.getTable().getName()==null) {
						col2.setTable(table);
					}
				}
			}
			else if(e instanceof MinorThan) {
				MinorThan eq = (MinorThan) e;
				Expression col = ((MinorThan) e).getLeftExpression();
				if(col instanceof Column) {
					Column col2 = (Column) col;
					if(col2.getTable().getName()==null) {
						col2.setTable(table);
					}
				}
			}
			else if (e instanceof MinorThanEquals) {
				MinorThanEquals eq = (MinorThanEquals) e;
				Expression col = ((MinorThanEquals) e).getLeftExpression();
				if(col instanceof Column) {
					Column col2 = (Column) col;
					if(col2.getTable().getName()==null) {
						col2.setTable(table);
					}
				}
			}
		}
		List<Expression> deleteExpList = SchemaStructure.deleteMap.get(table.getName());
		if(deleteExpList == null) {
			SchemaStructure.deleteMap = new HashMap<>();
			List<Expression> temp =  new ArrayList<Expression>();
			temp.addAll(expList);
			SchemaStructure.deleteMap.put(table.getName(), temp);		
		} else {
			SchemaStructure.deleteMap.get(table.getName()).addAll(expList);
		}
	}

}
