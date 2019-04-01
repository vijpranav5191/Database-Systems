package utils;

import java.util.ArrayList;
import java.util.List;

import iterators.DefaultIterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
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
	
	
	public static List<Expression> getExpressionForSelectionPredicate(Table table, List<ColumnDefs> cdefs, List<Expression> expressions)
	{
			List<Expression> lst = new ArrayList<Expression>();
			for(Expression expression : expressions)
			{
				if(expression instanceof EqualsTo && expression.toString().split(" ")[2].split("\\.").length == 2)
					continue;
//				System.out.println(" expresson " + expression);
				String part = expression.toString().split(" ")[0];
				if(part.split("\\.").length == 2)
				{
					for(ColumnDefs cd : cdefs)	
					{
//						System.out.println(" cd "+cd.cdef.getColumnName());
						if(  part.split("\\.")[1].equals(cd.cdef.getColumnName()) )
						{ 
//							System.out.println(" inide asds ");
							lst.add(expression);
						}
					}
				}
				else
				{ 
					for(ColumnDefs cd : cdefs){	
						if(  part.equals(cd.cdef.getColumnName()) )
						{
							lst.add(expression);
						}
					}
				}
			}
			if(lst != null && lst.size() > 0) {
				for(Expression exp : lst) {
					SchemaStructure.whrexpressions.remove(exp);
				}
			}
			return lst;
	}
	
	public static Expression getExpressionForJoinPredicate(List<String> leftcdefs,List<String> rightcdefs, List<Expression> expressions){
		Expression result = null; 
		if(expressions != null) {
			for(Expression exp: expressions) {
				if(exp instanceof EqualsTo) {
					EqualsTo equalTo = (EqualsTo) exp;
					String[] left = equalTo.getLeftExpression().toString().split("\\.");
					String[] right = equalTo.getRightExpression().toString().split("\\.");
					
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
	
	
	private static Boolean isContainingExpression(String[] exp, List<String> cdefs) {
		for(String value: exp) {
			for(String cdef: cdefs) {
				String[] split = cdef.split("\\.");
				for(String s: split) {
					if(value.equals(s)) {
						return true;
					}
				}
			}
		}
		
		return false;
	}
	
	
}
