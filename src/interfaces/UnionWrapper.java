package interfaces;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import iterators.DefaultIterator;
import iterators.JoinIterator;
import iterators.ResultIterator;
import iterators.SelectionIterator;
import iterators.TableScanIterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Union;
import queryexec.SelectWrapper;

public class UnionWrapper {
	
	Union union;
	List<PlainSelect> plainsel;
	private Expression whereExp;
	private List<SelectItem> selectItems;
	private List<Join> joins;
	private HashSet<Map<String, PrimitiveValue>> resultList;
	public UnionWrapper(Union union) {
		// TODO Auto-generated constructor stub
		this.union = union;
	}

	public void parse() {
		// TODO Auto-generated method stub
		plainsel = union.getPlainSelects();
		resultList = new HashSet<>();
		for (PlainSelect plainSelect : plainsel) {
			parseSelunion(plainSelect);
		}
		printresl(resultList);
	}
	
	private void printresl(HashSet<Map<String, PrimitiveValue>> resultList2) {
		// TODO Auto-generated method stub
		for (Map<String, PrimitiveValue> map : resultList2) {
			if(map != null) {
				System.out.println(map.toString());
			}
		}
	}

	public HashSet<PlainSelect> parseSelunion(PlainSelect plainselect){
		DefaultIterator iter = null;
		
		FromItem fromItem = plainselect.getFromItem();
		selectItems = plainselect.getSelectItems();
		whereExp = plainselect.getWhere();
		if(fromItem instanceof Table) {
			Table table = (Table) fromItem;
			iter = new TableScanIterator(table, true);
		}
		DefaultIterator result = iter;
		while(iter.hasNext()) {
			if((joins=plainselect.getJoins())!=null){
				for (Join join : joins) {
					FromItem item = join.getRightItem();
					if(item instanceof Table ) {
						DefaultIterator iter2 = new TableScanIterator((Table) item, false);
						result = new JoinIterator(result, iter2, join);
					}
					//System.out.println(result.next());
				}
			}
			if (this.whereExp != null) {
				result = new SelectionIterator(result, this.whereExp);
			}
//			if(this.selectItems != null ) {
//				result = new ProjectionIterator(result, this.selectItems);
//			}
//			ResultIterator res = new ResultIterator(result);
//			while(res.hasNext()) {
//				res.next();
	//			}
			while (result.hasNext()) {
				if(result.next()!=null)
					resultList.add(result.next());
			}

		}
		return null;
	}

}
