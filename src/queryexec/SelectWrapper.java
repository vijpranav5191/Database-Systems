package queryexec;

import java.util.List;
import iterators.DefaultIterator;
import iterators.HashJoinIterator;
import iterators.JoinIterator;
import iterators.OrderByIterator;
import iterators.ProjectionIterator;
import iterators.ResultIterator;
import iterators.SelectionIterator;
import iterators.SortMergeIterator;
import iterators.TableScanIterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;


public class SelectWrapper{
	private PlainSelect plainselect;
	private List<SelectItem> selectItems;	
	private Expression whereExp;
	private List<Join> joins;
	private List<OrderByElement> orderbyelement;
	
	public SelectWrapper(PlainSelect plainselect){
		this.plainselect = plainselect;
	}
	
	public void parse() {
		DefaultIterator iter = null;
	
		FromItem fromItem = this.plainselect.getFromItem();
		this.selectItems = this.plainselect.getSelectItems();
		this.whereExp = this.plainselect.getWhere();
		this.orderbyelement = this.plainselect.getOrderByElements();
		
		if(fromItem instanceof Table) {
			Table table = (Table) fromItem;
			iter = new TableScanIterator(table, true);
		}
		
		DefaultIterator result = iter;
		while(iter.hasNext()) {
			if((this.joins = this.plainselect.getJoins()) != null){
				for (Join join : joins) {
					FromItem item = join.getRightItem();
					if(item instanceof Table ) {
						DefaultIterator iter2 = new TableScanIterator((Table) item, false);
						result = new JoinIterator(result, iter2, join);
					}
				}
			}
			if (this.whereExp != null) {
				result = new SelectionIterator(result, this.whereExp);
			}
			
			if(this.whereExp != null) {
				result = new OrderByIterator(this.orderbyelement, result);
			}
			
			if(this.selectItems != null ) {
				result = new ProjectionIterator(result, this.selectItems, (Table) fromItem);
			}
			ResultIterator res = new ResultIterator(result);
			while(res.hasNext()) {
				res.next();
			}
		}
	}
}





