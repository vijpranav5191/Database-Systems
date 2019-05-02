package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;
import objects.ColumnDefs;

public class Utils {

	public static List<Expression> getExpressionForSelectionPredicate(Table table, List<ColumnDefs> cdefs,
			List<Expression> expressions) {
		List<Expression> lst = new ArrayList<Expression>();
		for (Expression expression : expressions) {
			if (expression instanceof EqualsTo)
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
}
