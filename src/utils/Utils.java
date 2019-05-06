package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.Index;
import objects.ColumnDefs;
import objects.SchemaStructure;

public class Utils {

	public static List<Expression> getExpressionForSelectionPredicate(Table table, List<ColumnDefs> cdefs,
			List<Expression> expressions) {
		List<Expression> lst = new ArrayList<Expression>();
		for (Expression expression : expressions) {
			if (expression instanceof EqualsTo && ((EqualsTo) expression).getRightExpression() instanceof Column)
				continue;
			String part = expression.toString().split(" ")[0];
			if (part.split("\\.").length == 2) {
				if (cdefs.contains(part)) {
					lst.add(expression);
				}
			} else {
				String val = (String) (table + "." + part);
				for (ColumnDefs cd : cdefs) {
					if (val.equals(cd.cdef.getColumnName())) {
						lst.add(expression);
					}
				}
			}
		}

		return lst;
	}

	public static List<Expression> getExpressionForJoinPredicate(Table table, List<ColumnDefs> cdefs,
			List<Expression> expressions) {
		return null;
	}

	public static List<Expression> splitAndClauses(Expression e) {
		List<Expression> ret = new ArrayList<Expression>();
		if (e != null) {
			if (e instanceof AndExpression) {
				AndExpression a = (AndExpression) e;
				ret.addAll(splitAndClauses(a.getLeftExpression()));
				ret.addAll(splitAndClauses(a.getRightExpression()));
			} else {
				ret.add(e);
			}
		}
		return ret;
	}
	public static List<Expression> splitWhereClauses(Expression e) {
		List<Expression> lstResult = new ArrayList<Expression>();
		
		if(e instanceof EqualsTo || e instanceof GreaterThan || e instanceof GreaterThanEquals || e instanceof MinorThan || e instanceof MinorThanEquals )
		{
			lstResult.add(e);
			return lstResult;
		}
		if( e instanceof AndExpression )
		{
			return splitAndClauses(e);
		}
		if( e instanceof OrExpression)
		{
			OrExpression exp =  (OrExpression) e;
			Expression left  = exp.getLeftExpression();
			Expression right = exp.getRightExpression();
			List<Expression> leftLst = splitAndClauses(left);
			List<Expression> rightLst = splitAndClauses(right);
			
			Expression e1 = leftLst.get(leftLst.size()-1);
			leftLst.remove(leftLst.size()-1);
			Expression e2 = rightLst.get(0);
			rightLst.remove(0);
			
			lstResult.add( new OrExpression(e1 , e2) );
			
			lstResult.addAll(leftLst);
			lstResult.addAll(rightLst);
		}
		return lstResult;
	}
	
	public static List<Expression> splitOrClauses(Expression e) {
		List<Expression> ret = new ArrayList<Expression>();
		if (e != null) {
			if (e instanceof OrExpression) {
				OrExpression a = (OrExpression) e;
				ret.addAll(splitOrClauses(a.getLeftExpression()));
				ret.addAll(splitOrClauses(a.getRightExpression()));
			} else {
				ret.add(e);
			}
		}
		return ret;
	}
	
	public static List<Expression> result = new ArrayList<>();
	
	public static void splitOrClauses2(Expression e) {
		
		if( !(e instanceof OrExpression ) )
		{
			result.add(e);
			return;
		}
		OrExpression orExp = (OrExpression) e;
		result.add(orExp.getRightExpression());
		splitOrClauses2(orExp.getLeftExpression());	
	}
	public static Set<Expression> splitAllClauses(Expression e) {
		Set<Expression> ret = new HashSet<Expression>();
//		ret.addAll(splitAndClauses(e));
		for(Expression a : splitAndClauses(e)) {
			if(a instanceof OrExpression) 
			{
				ret.remove(a);
				splitOrClauses2(a);
				ret.addAll(result);
				result.clear();
			}
			else {
				ret.add(a);
			}
		}
		return ret;
	}
	
	public static String hashString(String message) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		MessageDigest digest = MessageDigest.getInstance("MD5");
		byte[] hashedBytes = digest.digest(message.getBytes("UTF-8"));
		return convertByteArrayToHexString(hashedBytes);
	}

	private static String convertByteArrayToHexString(byte[] arrayBytes) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < arrayBytes.length; i++) {
			stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return stringBuffer.toString();
	}

	public static String getDate(Function func) {
		return func.getParameters().getExpressions().get(0).toString();
	}

	public static Expression conquerExpression(List<Expression> elist) {
		if (elist.size() == 2) 
		{
			AndExpression and = new AndExpression();
			and.setLeftExpression(elist.get(0));
			and.setRightExpression(elist.get(1));
			return and;
		}
		Expression result = elist.get(0);

		for (int i = 1; i < elist.size(); i++) 
		{
			AndExpression and = new AndExpression();
			and.setLeftExpression(result);
			and.setRightExpression(elist.get(i));
			result = and;
		}

		return result;
	}

	private static Expression recursion(List<Expression> elist, int index, Expression result) {
		if (index == elist.size() + 2) {
			return result;
		}

		AndExpression and = new AndExpression();
		if (index < elist.size()) {
			System.out.println(" + " + result);
			and.setLeftExpression(result);
			and.setRightExpression(elist.get(index));
			recursion(elist, index + 1, and);

		}
		return result;
	}
	
	public static boolean isContainingColumns(String leftExpression, List<String> columns) {
		for(String column: columns) {
			if(column.equals(leftExpression)) {
				return true;
			}
			String[] columnSplt = column.split("\\.");
			for(String split: columnSplt) {
				if(split.equals(leftExpression)) {
					return true;
				}
			}
		}
		return false;
	}
	
	public static boolean isFileExists(String path) {
		File tmpDir = new File(path);
	    return tmpDir.exists();
	}
	
	public static void createDirectory(String folderName) {
		File f = new File(folderName);
		f.mkdirs();
	}
	
	private static RandomAccessFile raf = null;
	
	public static String readTuple(String path, int index) {
		String tuple = null;
		try {
			if(raf == null) {
				raf = new RandomAccessFile(path, "rw");
			}
			raf.seek(index);
			tuple =  raf.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tuple;
	}
	
	public static boolean isPrimaryKey(String key, List<Index> indexes) {
		for(Index index: indexes) {
			if(index.getType().equals(Constants.PRIMARY_KEY)) {
				for(String primaryKey: index.getColumnsNames()) {
					if(key.equals(primaryKey)) {
						return true;
					}
				}
			}
		}
		return false;
	}
	
	public static void createPrecedenceList() {// sorted by size
		String[] tables = {"LINEITEM", "ORDERS", "PARTSUPP", "CUSTOMER","PART", "SUPPLIER", "NATION", "REGION",};
		Integer[] sizeOfTable = {725, 164, 113, 23, 23, 3, 2, 1};
		for(int i = 0;i < tables.length;i++) {
			SchemaStructure.precedenceMap.put(tables[i], sizeOfTable[i]);
		}
	}
	
	public static boolean isHoldingPrecedence(Table left, Table right) {
		if(SchemaStructure.precedenceMap.get(right.getName()) < SchemaStructure.precedenceMap.get(left.getName())) {
			return true;
		}
		return false;
	}

	private static RandomAccessFile raf_1 = null;
	public static BufferedReader getInputStreamBySeek(String path, int seekPosition) throws IOException {
		try {
			if(raf_1 == null) {
				raf_1 = new RandomAccessFile(path, "rw");
			}
			raf_1.seek(seekPosition);
		} catch (IOException e) {
			e.printStackTrace();
		}
		InputStream is = Channels.newInputStream(raf_1.getChannel());
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		return br;
	}

	public static Collection<? extends String> splitExpCols(Expression exp) {
		// TODO Auto-generated method stub
		List<String> temp = new ArrayList<>();
		if(exp instanceof BinaryExpression) {
			temp.addAll(splitExpCols(((BinaryExpression) exp).getLeftExpression()));
			temp.addAll(splitExpCols(((BinaryExpression) exp).getRightExpression()));
		}else if(exp instanceof Column) {
			exp = ((Column)exp);
			temp.add(((Column) exp).getWholeColumnName());
		}
		return temp;
	}
//	public static List<String> result2 = new ArrayList<>();
//	public static void splitBinaryFunc(Expression e) {
//		
//		if( !(e instanceof BinaryExpression ) )
//		{
//			if(e instanceof Column)
//			{
//				result2.add(((Column) e).getWholeColumnName());
//				return;
//			}
//		}
//		BinaryExpression binExp = (BinaryExpression) e;
//		if(binExp.getRightExpression() instanceof Column)
//		{
//			Column x = (Column) binExp.getRightExpression();
//			result2.add(x.getWholeColumnName());
//		}
//		splitBinaryFunc(binExp.getLeftExpression());	
//	}
	
	public static HashSet<String> splitExpCols2(Expression exp) {
		// TODO Auto-generated method stub
		HashSet<String> result1 = new HashSet<>();
		if(exp instanceof Column)
		{
			result1.add(exp.toString());
			return result1;
		}
		if(exp instanceof BinaryExpression){
			BinaryExpression binExp = (BinaryExpression) exp;
			
			Expression leftB =  binExp.getLeftExpression();
			Expression rightB = binExp.getRightExpression();
		
			if(leftB instanceof Column)
				result1.add(((Column) leftB).getWholeColumnName());
			else if (leftB instanceof BinaryExpression)
				result1.addAll(splitExpCols2(leftB));
			
			if(rightB instanceof Column)
				result1.add(((Column) rightB).getWholeColumnName());
			else if (rightB instanceof BinaryExpression) 
				result1.addAll(splitExpCols2(rightB));
		}
		else if (exp instanceof CaseExpression) {
			CaseExpression ce = (CaseExpression) exp;
			List<WhenClause> whenClauses = ce.getWhenClauses();
			for(WhenClause e: whenClauses) {
				Expression whenExp = e.getWhenExpression();
				Set<Expression> splitlist = Utils.splitAllClauses(whenExp);
				for(Expression a : splitlist) {
					result1.addAll(Utils.splitExpCols2(a));
				}
			}
		}
		return result1;
	}
}






