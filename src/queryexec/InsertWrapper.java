package queryexec;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.insert.Insert;
import objects.SchemaStructure;

public class InsertWrapper {
	Insert insert;
	
	public InsertWrapper(Insert insert) {
		this.insert = insert;
	}

	public void insertValues() {
		ExpressionList values = (ExpressionList) this.insert.getItemsList();
		List<Expression> exps = values.getExpressions();
		List<PrimitiveValue> tuple = new ArrayList<>();
		for(Expression exp: exps) {
			PrimitiveValue pm = (PrimitiveValue) exp;
			tuple.add(pm);
		}
		List<List<PrimitiveValue>> tuples = SchemaStructure.insertTuples.getOrDefault(insert.getTable().getName(), new ArrayList<>());
		tuples.add(tuple);
		SchemaStructure.insertTuples.put(insert.getTable().getName(), tuples);
	}
}
