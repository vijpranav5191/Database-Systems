package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import iterators.DefaultIterator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import objects.ColumnDefs;
import objects.SchemaStructure;

public class Optimzer {
//	public static DefaultIterator optimize(Table table, DefaultIterator iterator) {
//		List<Expression> list = SchemaStructure.whrexpressions;
//		if(list!=null && list.size() > 0) {
//			List<Expression> temp = new ArrayList<Expression>();
//			for(Expression e: list) {
//				Expression leftExpression = e
//			}
//		}
//		return null;
//	}
	
	
	public static List<Expression> getExpressionForSelectionPredicate(Table table, List<ColumnDefs> cdefs, List<Expression> expressions){
		List<Expression> lst = new ArrayList<Expression>();

		for(Expression expression : expressions)
		{
			if(expression instanceof EqualsTo && ((EqualsTo) expression).getRightExpression() instanceof Column)
				continue;
//			System.out.println(" expresson " + expression);
			
			BinaryExpression bExp = (BinaryExpression) expression;
			
			if(expression instanceof OrExpression )
			{

				OrExpression orExp = (OrExpression) expression;
				BinaryExpression bExp1 = (BinaryExpression) orExp.getLeftExpression();
				Column col1 = (Column) bExp1.getLeftExpression();
				if(col1.getTable().toString().equals(table.toString()))
				{
					lst.add(expression);
				}

			}
			else {
				Column col = (Column) bExp.getLeftExpression();
				if(col.getTable().toString().equals(table.toString()))
				{
					lst.add(expression);
				}
			}
				//				lst.add(expression);
			

		}
		if(lst != null && lst.size() > 0) {
			for(Expression exp : lst) {
				SchemaStructure.whrexpressions.remove(exp);
			}
		}
		return lst;
}
	
	


	public static Expression getExpressionForJoinPredicate(List<String> leftcdefs, List<String> rightcdefs, List<Expression> expressions){
		Expression result = null; 
		if(expressions != null) {
			for(Expression exp: expressions) {
				if(exp instanceof EqualsTo) {
					EqualsTo equalTo = (EqualsTo) exp;
					//String[] left = equalTo.getLeftExpression().toString().split("\\.");
					//String[] right = equalTo.getRightExpression().toString().split("\\.");
					String left = equalTo.getLeftExpression().toString();
					String right = equalTo.getRightExpression().toString();	
					if(isContainingExpression(left, leftcdefs) && isContainingExpression(right, rightcdefs)) {
						result = exp;
						break;
					}
					if(isContainingExpression(right, leftcdefs) && isContainingExpression(left, rightcdefs)) {
						result = exp;
						break;
					}
				}  
			}
		}
		if(result != null) {
			SchemaStructure.whrexpressions.remove(result);
		}
		return result;
	}
	
	
	private static Boolean isContainingExpression(String column, List<String> cdefs) {
//		for(String value: exp) {
//			for(String cdef: cdefs) {
//				String[] split = cdef.split("\\.");
//				for(String s: split) {
//					if(value.equals(s)) {
//						return true;
//					}
//				}
//			}
//		}
//		return false;
		
		for(String cdef: cdefs) {
			if(cdef.equals(column)) {
				return true;
			}
		}
		return false;
	}

	public static PrimitiveValue evaluateExpression(Map<String, PrimitiveValue> scope, Expression where) throws Exception {
		Eval eval = new Eval() {
			public PrimitiveValue eval(Column col){
				String name = col.getColumnName();
				if(col.getTable() != null && col.getTable().getName() != null)
				{
			        name = col.getTable().getName() + "." + col.getColumnName();
			        return scope.get(name);
			    } 
				else {
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
