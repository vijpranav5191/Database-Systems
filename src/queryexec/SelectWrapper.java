package queryexec;

import java.io.IOException;
import java.util.List;
import iterators.DefaultIterator;
import iterators.GroupByIterator;
import iterators.HavingIterator;

import iterators.HashJoinIterator;

import iterators.JoinIterator;
import iterators.LimitIterator;
import iterators.ProjectionIterator;
import iterators.ResultIterator;
import iterators.SelectionIterator;
import iterators.SortMergeIterator;
import iterators.TableScanIterator;
import iterators.orderExternalIterator;
import iterators.orderIterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import objects.SchemaStructure;


public class SelectWrapper{
	private PlainSelect plainselect;
	private List<SelectItem> selectItems;	
	private Expression whereExp;
	private List<Column> groupBy;
	private List<Join> joins;
	private List<OrderByElement> orderBy;
	private boolean flagOrderBy;
	private Limit limit;
	private Expression having;

	public SelectWrapper(PlainSelect plainselect){
		this.plainselect = plainselect;
	}
	
	public void parse() throws IOException {
		DefaultIterator iter = null;
	
		FromItem fromItem = this.plainselect.getFromItem();
		this.selectItems = this.plainselect.getSelectItems();
		this.whereExp = this.plainselect.getWhere();
		this.groupBy = this.plainselect.getGroupByColumnReferences();
		this.orderBy = this.plainselect.getOrderByElements();
		this.limit = this.plainselect.getLimit();
		this.having = this.plainselect.getHaving();
		this.flagOrderBy = false;
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
			
			if (this.groupBy!=null) {
				for(Column key : groupBy) {
					String xKey = key.getColumnName();
					if(xKey.split("\\.").length == 1){
						key.setTable(SchemaStructure.tableMap.getOrDefault(xKey, (Table) fromItem));
					}
					
				}
				result = new GroupByIterator(result, this.groupBy, (Table) fromItem, this.selectItems);
			}

			if(this.having!=null) {
				result = new HavingIterator(result, this.having, this.selectItems);
			}
			if(this.orderBy != null){
				for(OrderByElement key : orderBy){
				 	String xKey = key.getExpression().toString();
					if(xKey.split("\\.").length == 1){
						Column cCol = new Column(SchemaStructure.tableMap.getOrDefault(xKey, (Table) fromItem) , xKey);
						key.setExpression(cCol);
					}
					
				}
				if(this.flagOrderBy == false) {
					result = new orderIterator(result,this.orderBy);
				} else{
					result = new orderExternalIterator(result,this.orderBy, (Table) fromItem);
				}
			}
			
			if(this.selectItems != null ) {
				result = new ProjectionIterator(result, this.selectItems, (Table) fromItem , this.groupBy);
			}
			
			if(this.limit != null) {
				result = new LimitIterator(result, this.limit);
			}
			
			ResultIterator res = new ResultIterator(result);
			while(res.hasNext()) {
				res.next();
			}
		}
	}
}