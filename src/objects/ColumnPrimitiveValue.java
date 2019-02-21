package objects;

import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.PrimitiveType;

public class ColumnPrimitiveValue implements PrimitiveValue{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void accept(ExpressionVisitor arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public PrimitiveType getType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean toBool() throws InvalidPrimitive {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public double toDouble() throws InvalidPrimitive {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long toLong() throws InvalidPrimitive {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toRawString() {
		// TODO Auto-generated method stub
		return null;
	}

}
