package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class EvaluateUtils{
 
	public static Boolean evaluate(List<PrimitiveValue> scope, Expression where, Map<String, Integer> mapperColumn) throws Exception {
		Eval eval = new Eval() {
			public PrimitiveValue eval(Column col){
				String name = col.getColumnName();
				if(col.getTable() != null && col.getTable().getName() != null){
			        name = col.getTable().getName() + "." + col.getColumnName();
			        return scope.get(mapperColumn.get(name));
			    } else {
			    	for(String key: mapperColumn.keySet()) {
				    	if(key.split("\\.")[1].equals(name)) {
				    		return scope.get(mapperColumn.get(name));
				    	}
				    }
				} 
				return scope.get(mapperColumn.get(name));
			}
			
			public PrimitiveValue eval(Function func){
				String name = func.getName();
				
				if(name.equals("DATE")) {
					String dateString = Utils.getDate(func);
					return new DateValue(dateString.substring(1, dateString.length() - 1));
				}
				
				List<Expression> expList = new ArrayList<>();
				String namefun = func.getName();
				String key= null;
				if(func.getParameters() != null) {
					expList = func.getParameters().getExpressions(); //check this later
					StringBuilder sb = new StringBuilder();
					for(Expression exp : expList) {
						sb.append(exp.toString());
					}
					key = namefun + "(" + sb.toString() + ")";
				}
				else {
					if(func.isAllColumns()) {
						key = "COUNT(*)";
					}
				}
    			return scope.get(mapperColumn.get(name));
			}
		};
		return eval.eval(where).toBool();
	}
	
	public static PrimitiveValue evaluateExpression(List<PrimitiveValue> scope, Expression where, Map<String, Integer> mapperColumn) throws Exception {
		Eval eval = new Eval() {
			
			public PrimitiveValue eval(Column col){
				String name = col.getColumnName();
				if(col.getTable() != null && col.getTable().getName() != null){
			        name = col.getTable().getName() + "." + col.getColumnName();
			        return scope.get(mapperColumn.get(name));
			    }
				else {
			    	for(String key: mapperColumn.keySet()) {
			    		if(key.split("\\.")[1].equals(name)) {
			    			return scope.get(mapperColumn.get(key));
			    		}
			    	}
				}
				return scope.get(mapperColumn.get(name));
			}
		};
		return eval.eval(where);
	}
}