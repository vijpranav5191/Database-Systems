package queryexec;

import java.util.List;
import iterators.DefaultIterator;
import iterators.JoinIterator;
import iterators.ProjectionIterator;
import iterators.ResultIterator;
import iterators.SelectionIterator;
import iterators.TableScanIterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;


public class SelectWrapper{
	private PlainSelect plainselect;
	private List<SelectItem> selectItems;	
	private Expression whereExp;
	private List<Join> joins;
	public SelectWrapper(PlainSelect plainselect){
		this.plainselect = plainselect;
	}
	
	public void parse() {
		DefaultIterator iter = null;
	
		FromItem fromItem = this.plainselect.getFromItem();
		this.selectItems = this.plainselect.getSelectItems();
		this.whereExp = this.plainselect.getWhere();
		if(fromItem instanceof Table) {
			Table table = (Table) fromItem;
			iter = new TableScanIterator(table);
		}
		DefaultIterator result = iter;
		while(iter.hasNext()) {
			if((this.joins = this.plainselect.getJoins()) != null){
				for (Join join : joins) {
					FromItem item = join.getRightItem();
					if(item instanceof Table ) {
						DefaultIterator iter2 = new TableScanIterator((Table) item);
						result = new JoinIterator(result, iter2, join);
					}
				}
			}
			if (this.whereExp != null) {
				result = new SelectionIterator(result, this.whereExp);
			}
			if(this.selectItems != null ) {
				result = new ProjectionIterator(result, this.selectItems);
			}
			ResultIterator res = new ResultIterator(result);
			while(res.hasNext()) {
				res.next();
			}
		}
	}
}





