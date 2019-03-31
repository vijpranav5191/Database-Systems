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
				String name = col.getColumnName();
				if(col.getTable() != null && col.getTable().getName() != null){
			        name = col.getTable().getName() + "." + col.getColumnName();
			        return scope.get(name);
			    } else {
			    	for(String key: scope.keySet()) {
			    		if(key.split("\\.")[1].equals(name)) {
			    			return scope.get(key);
			    		}
			    	}
			    }
				return scope.get(name);
			}
		};
		return eval.eval(where).toBool();
	}
	
//	public static PrimitiveValue evaluateExpression(Map<String, PrimitiveValue> scope) throws Exception {
//		Eval eval = new Eval() {
//			public PrimitiveValue eval(Column col){
//				String name = col.getColumnName();
//				if(col.getTable() != null && col.getTable().getName() != null){
//			        name = col.getTable().getName() + "." + col.getColumnName();
//			        return scope.get(name);
//			    } else {
//			    	for(String key: scope.keySet()) {
//			    		if(key.split("\\.")[1].equals(name)) {
//			    			return scope.get(key);
//			    		}
//			    	}
//				}
//				return scope.get(name);
//			}
//		};
//		return eval.eval(where);
//	}

	
	
	public static PrimitiveValue evaluateExpression(Map<String, PrimitiveValue> scope, Expression where) throws Exception {
		Eval eval = new Eval() {
			public PrimitiveValue eval(Column col){
				String name = col.getColumnName();
				if(col.getTable() != null && col.getTable().getName() != null){
			        name = col.getTable().getName() + "." + col.getColumnName();
			        return scope.get(name);
			    } else {
			    	for(String key: scope.keySet()) {
			    		if(key.split("\\.")[1].equals(name)) {
			    			return scope.get(key);
			    		}
			    	}
				}
				return scope.get(name);
			}
		};
		return eval.eval(where);
	}
	
}