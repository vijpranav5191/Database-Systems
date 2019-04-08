package iterators;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import FileUtils.mFileWriter;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.select.Join;
import utils.Config;
import utils.Utils;

public class TwoPassHashJoin implements DefaultIterator{
	
	DefaultIterator leftIterator;
	DefaultIterator rightIterator;
	List<String> leftBucket;
	List<String> rightBucket;
	
	Map<String, PrimitiveValue> rightTuple;
	String rightExpression = null;
	String leftExpression = null;
	
	Join join;
	List<String> columns;
	Map<String, PrimitiveValue> nextResult;
	
	
	
	public TwoPassHashJoin(DefaultIterator leftIterator, DefaultIterator rightIterator, Join join){
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.join = join;
		this.columns = new ArrayList<String>();
		checkExpressionSwitch();
		createBucket(this.leftIterator, "left", this.leftExpression);
		createBucket(this.rightIterator, "right", this.rightExpression);
		this.nextResult = getNextIter();
	}

	private void createBucket(DefaultIterator iterator, String tag, String expression) {// tag is "left" / "right"
		while(iterator.hasNext()) {
			Map<String, PrimitiveValue> map = iterator.next();
			try {
				mFileWriter fileWriter = new mFileWriter(Config.bucket_location, 
						Utils.hashString(map.get(expression).toString()) + "_" + tag,
						iterator.getColumns());
				fileWriter.writeNext(map);
				fileWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void checkExpressionSwitch() {
		if(this.join.getOnExpression() instanceof EqualsTo) {
			EqualsTo exp = (EqualsTo) this.join.getOnExpression();
			this.leftExpression = exp.getLeftExpression().toString(); // need to check this approach
			this.rightExpression = exp.getRightExpression().toString();
			if(!Utils.isContainingColumns(leftExpression, this.leftIterator.getColumns())) {
				this.leftExpression = exp.getRightExpression().toString(); // need to check this approach
				this.rightExpression = exp.getLeftExpression().toString();
			}
		}	
	}
	
	@Override
	public boolean hasNext() {
		if(this.nextResult != null) {
			return true;
		}
		return false;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		Map<String, PrimitiveValue> temp = this.nextResult;
		this.nextResult = getNextIter();
		return temp;
	}

	private Map<String, PrimitiveValue> getNextIter() {
		
		return null;
	}

	@Override
	public void reset() {
		this.leftIterator.reset();
		this.rightIterator.reset();
	}
	

	@Override
	public List<String> getColumns() {
		if(this.columns.size() == 0) {
			this.columns.addAll(this.leftIterator.getColumns());
			this.columns.addAll(this.rightIterator.getColumns());
		}
		return this.columns;
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return null;
	}


}
