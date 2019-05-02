package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.Index;
import objects.ColumnDefs;
import objects.SchemaStructure;

public class IndexSelectionIterator implements DefaultIterator{
	
	DefaultIterator iterator;
	DefaultIterator iteratorTemp;
	List<ColumnDefinition> cdefs;
	@SuppressWarnings("unlikely-arg-type")
	public IndexSelectionIterator( DefaultIterator iterator, Table table , String columnName) {
			
		
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
