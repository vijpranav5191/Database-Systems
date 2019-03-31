package utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public class Utils {

	public static List<Expression> splitAndClauses(Expression e) {
		List<Expression> ret = new ArrayList<Expression>();
		if(e instanceof AndExpression){
			AndExpression a = (AndExpression)e;
			ret.addAll(splitAndClauses(a.getLeftExpression()));
			ret.addAll(splitAndClauses(a.getRightExpression()));
		} else {
			ret.add(e);
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
	        stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16)
	                .substring(1));
	    }
	    return stringBuffer.toString();
	}
	
	public static String getDate(Function func) {
		return func.getParameters().getExpressions().get(0).toString();	
	}
}
