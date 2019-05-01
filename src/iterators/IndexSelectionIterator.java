package iterators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import com.sun.javafx.applet.ExperimentalExtensions;
import com.sun.javafx.fxml.expression.BinaryExpression;
import com.sun.xml.internal.bind.v2.schemagen.xmlschema.Schema;

import bPlusTree.BPlusTree;
import bPlusTree.BPlusTreeBuilder;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
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

public class IndexSelectionIterator implements DefaultIterator{
	
	DefaultIterator iterator;
	DefaultIterator iteratorTemp;
	@SuppressWarnings("unlikely-arg-type")
	public IndexSelectionIterator(DefaultIterator iterator ,Table table , Expression exp) {
		// TODO Auto-generated constructor stub
		this.iteratorTemp = iterator;
		HashMap< String , List<Index>> indexMap = SchemaStructure.indexMap;
		if( exp instanceof GreaterThan   )
		{
			GreaterThan expGreaterThan = (GreaterThan) exp;
			Column leftExp = (Column)expGreaterThan.getLeftExpression();
			Column rightExp = (Column)expGreaterThan.getRightExpression();
			String leftColumnName = null;
			Table leftTableName = null;
			if( leftExp instanceof Column)
			{
				leftColumnName =   leftExp.getColumnName(); 
				leftTableName =  leftExp.getTable(); 
			}
			String rightColumnName = "";
			Table rightTableName = null;
			
			if( rightExp instanceof Column)
			{
				rightColumnName =  rightExp.getColumnName(); 
				rightTableName =  rightExp.getTable();
			}
			
			
			
			
			if(leftColumnName != null ) 
				function( indexMap , leftColumnName , leftTableName ,  exp );
			if(rightColumnName != null ) 
				function( indexMap , rightColumnName , rightTableName ,  exp );				
			
		}
		
		else if( exp instanceof GreaterThanEquals )
		{
			GreaterThanEquals expGreaterThan = (GreaterThanEquals) exp;
			Column leftExp = (Column) expGreaterThan.getLeftExpression();
			Column rightExp = (Column)expGreaterThan.getRightExpression();
			String leftColumnName = null;
			Table leftTableName = null;
			if( leftExp instanceof Column)
			{
				leftColumnName =   leftExp.getColumnName(); 
				leftTableName =  leftExp.getTable(); 
			}
			String rightColumnName = "";
			Table rightTableName = null;
			
			if( rightExp instanceof Column)
			{
				rightColumnName =  rightExp.getColumnName(); 
				rightTableName =  rightExp.getTable();
			}
		
			if(leftColumnName != null ) 
				function( indexMap , leftColumnName , leftTableName ,  exp );
			if(rightColumnName != null ) 
				function( indexMap , rightColumnName , rightTableName ,  exp );	
		}
		
		else if( exp instanceof MinorThan   )
		{
			MinorThan expMinorThan = (MinorThan) exp;
			Column leftExp = (Column) expMinorThan.getLeftExpression();
			Column rightExp = (Column) expMinorThan.getRightExpression();
			String leftColumnName = null;
			Table leftTableName = null;
			if( leftExp instanceof Column)
			{
				leftColumnName =   leftExp.getColumnName(); 
				leftTableName =  leftExp.getTable(); 
			}
			String rightColumnName = "";
			Table rightTableName = null;
			
			if( rightExp instanceof Column)
			{
				rightColumnName =  rightExp.getColumnName(); 
				rightTableName =  rightExp.getTable();
			}
			
			if(leftColumnName != null ) 
				function( indexMap , leftColumnName , leftTableName ,  exp );
			if(rightColumnName != null ) 
				function( indexMap , rightColumnName , rightTableName ,  exp );				
			
		}
		
		else if( exp instanceof MinorThanEquals )
		{
			MinorThanEquals expMinorThan = (MinorThanEquals) exp;
			Column leftExp = (Column) expMinorThan.getLeftExpression();
			Column rightExp = (Column)expMinorThan.getRightExpression();
			String leftColumnName = null;
			Table leftTableName = null;
			if( leftExp instanceof Column)
			{
				leftColumnName =   leftExp.getColumnName(); 
				leftTableName =  leftExp.getTable(); 
			}
			String rightColumnName = "";
			Table rightTableName = null;
			
			if( rightExp instanceof Column)
			{
				rightColumnName =  rightExp.getColumnName(); 
				rightTableName =  rightExp.getTable();
			}
		
			if(leftColumnName != null ) 
				function( indexMap , leftColumnName , leftTableName ,  exp );
			if(rightColumnName != null ) 
				function( indexMap , rightColumnName , rightTableName ,  exp );	
		}
		
		else if( exp instanceof EqualsTo )
		{
			EqualsTo expEquals = (EqualsTo) exp;
			Column leftExp = (Column) expEquals.getLeftExpression();
			Column rightExp = (Column)expEquals.getRightExpression();
			String leftColumnName = null;
			Table leftTableName = null;
			if( leftExp instanceof Column)
			{
				leftColumnName =   leftExp.getColumnName(); 
				leftTableName =  leftExp.getTable(); 
			}
			String rightColumnName = "";
			Table rightTableName = null;
			
			if( rightExp instanceof Column)
			{
				rightColumnName =  rightExp.getColumnName(); 
				rightTableName =  rightExp.getTable();
			}
		
			if(leftColumnName != null ) 
				function( indexMap , leftColumnName , leftTableName ,  exp );
			if(rightColumnName != null ) 
				function( indexMap , rightColumnName , rightTableName ,  exp );	
		}	
		
	}
	
	private void function(HashMap<String, List<Index>> indexMap, String columnName, Table tableName , Expression exp) {
		// TODO Auto-generated method stub
		List<Index> indexes =  indexMap.get( tableName.toString());
		if( indexes.contains(columnName))
		{
			this.iterator = BPlusTreeBuilder.searchByRange(iterator , tableName , exp );
		}
		else
		{
			this.iterator = new  SelectionIterator(iterator , exp);
		}
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return this.iterator.hasNext();
//		return false;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return this.iterator.next();
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.iterator = this.iteratorTemp;
		
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		
		return this.iterator.getColumns();
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return null;
	}

}
