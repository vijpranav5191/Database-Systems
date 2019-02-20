package utils;

import java.util.Map;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class EvaluateUtils{
 
	public static Boolean evaluate(Map<String, PrimitiveValue> scope, Expression where) throws Exception {
		Eval eval = new Eval() {
			public PrimitiveValue eval(Column col){
			      return scope.get(col.getColumnName());
			    }
		};
		return eval.eval(where).toBool();
	}
}