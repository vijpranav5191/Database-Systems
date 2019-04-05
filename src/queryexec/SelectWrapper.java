package queryexec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import iterators.DefaultIterator;
import iterators.GroupByIterator;
import iterators.HavingIterator;
import iterators.HashJoinIterator;
import iterators.JoinIterator;
import iterators.LimitIterator;
import iterators.OrderByIterator;
import iterators.ProjectionIterator;
import iterators.ResultIterator;
import iterators.SelectionIterator;
import iterators.SortMergeIterator;
import iterators.TableScanIterator;
import iterators.groupByExternal;
import iterators.newGroupBy;
import iterators.orderExternalIterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import objects.ColumnDefs;
import objects.SchemaStructure;
import utils.Config;
import utils.Optimzer;
import utils.Utils;



public class SelectWrapper	
{
	private PlainSelect plainselect;
	private List<SelectItem> selectItems;	
	private Expression whereExp;
	private List<Column> groupBy;
	private List<Join> joins;
	private List<OrderByElement> orderBy;


	private Limit limit;
	private Expression having;


	public SelectWrapper(PlainSelect plainselect)
	{
		this.plainselect = plainselect;
	}

	public void parse() throws Exception {
		DefaultIterator iter = null;
		FromItem fromItem = this.plainselect.getFromItem();
		this.selectItems = this.plainselect.getSelectItems();
		this.whereExp = this.plainselect.getWhere();
		this.groupBy = this.plainselect.getGroupByColumnReferences();
		this.orderBy = this.plainselect.getOrderByElements();
		this.limit = this.plainselect.getLimit();
		this.having = this.plainselect.getHaving();	
		SchemaStructure.whrexpressions = Utils.splitAndClauses(whereExp);

		//		Expression exp = Optimzer.getExpressionForJoinPredicate(SchemaStructure.tableMap.get(leftTable), SchemaStructure.schema.get(leftTable), SchemaStructure.tableMap.get(rightTable), SchemaStructure.schema.get(rightTable), SchemaStructure.whrexpressions);
		//		System.out.print(exp);
		//		
		//		List<Expression> temp = Optimzer.getExpressionForSelectionPredicate(SchemaStructure.tableMap.get(rightTable), SchemaStructure.schema.get(rightTable), SchemaStructure.whrexpressions);
		//		System.out.print(temp);


		if(fromItem instanceof Table) {
			Table table = (Table) fromItem;
			iter = new TableScanIterator(table);
			iter = pushDownSelectPredicate(table, iter);
		}

		DefaultIterator result = iter;

		while(result.hasNext()) {

			if((this.joins = this.plainselect.getJoins()) != null){
				for (Join join : joins) {
					FromItem item = join.getRightItem();
					if(item instanceof Table ) {
						Table rightTb = (Table) item;
						DefaultIterator iter2 = new TableScanIterator(rightTb);
						iter2 = pushDownSelectPredicate(rightTb, iter2);
						List<String> leftColumns = result.getColumns();
						List<String> rightColumns = iter2.getColumns();

						result = pushDownJoinPredicate(leftColumns, rightColumns, result, iter2, join);
					}
				}
			}

			if (this.whereExp != null) {
				List<Expression> tempExp = SchemaStructure.whrexpressions;
				if(tempExp != null && tempExp.size() > 0) {
					Expression exp = Utils.conquerExpression(tempExp);
					result = new SelectionIterator(result, exp);
				}
			}

			if (this.groupBy != null) {
				for(Column key : groupBy) {
					String xKey = key.getColumnName();
					if(key.getTable() == null){
						key.setTable(SchemaStructure.tableMap.getOrDefault(xKey, (Table) fromItem));
					}
				}
				if(Config.isInMemory) {
					result = new newGroupBy(result, this.groupBy, (Table) fromItem, this.selectItems);
				} else {
					result = new groupByExternal(result, this.groupBy, (Table) fromItem, this.selectItems);
				}
			}

			if(this.having!=null) {
				result = new HavingIterator(result, this.having, this.selectItems);
			}


			if(this.orderBy != null){
				for(OrderByElement key : this.orderBy){
					String xKey = key.getExpression().toString();
					if(xKey.split("\\.").length == 1){
						Table defTable = (Table) fromItem;
						Column cCol = new Column();
						if(isContainingColumn(xKey, SchemaStructure.schema.get(defTable.getName()))) {
							cCol.setColumnName(xKey);
							cCol.setTable(SchemaStructure.tableMap.getOrDefault(xKey, defTable));
						} else {
							cCol.setColumnName(xKey);
							cCol.setTable(SchemaStructure.tableMap.getOrDefault(xKey, null));
						}
						key.setExpression(cCol);
					}
				}
				if(Config.isInMemory) {
					result = new OrderByIterator(this.orderBy, result);
				} else {
					result = new orderExternalIterator(result, this.orderBy, (Table) fromItem , this.selectItems);
				}
			}


			if(this.having!=null) {
				result = new HavingIterator(result, this.having, this.selectItems);
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

	private boolean isContainingColumn(String xKey, List<ColumnDefs> list) {
		for(ColumnDefs cdef: list) {
			if(cdef.cdef.getColumnName().equals(xKey)) {
				return true;
			}
		}
		return false;
	}

	public DefaultIterator optimize(DefaultIterator root) {

		if(root instanceof SelectionIterator) {
			SelectionIterator selIter = (SelectionIterator) root;
			if(selIter.getChildIter() instanceof JoinIterator) {
				DefaultIterator joiniter = (JoinIterator) selIter.getChildIter();	
				Expression selExp = selIter.getWhereExp();
			}
		}
		return null;
	}

	public List<Expression> splitAndClauses(Expression e){
		List<Expression> ret = new ArrayList<>();
		if(e instanceof AndExpression){
			AndExpression a = (AndExpression)e;
			ret.addAll(splitAndClauses(a.getLeftExpression()));
			ret.addAll(splitAndClauses(a.getRightExpression()));
		} else {
			ret.add(e);
		}
		return null;
	} 

	public DefaultIterator pushDownSelectPredicate(Table table, DefaultIterator iter) {
		List<Expression> tempExp = Optimzer.getExpressionForSelectionPredicate(table, SchemaStructure.schema.get(table.getName()), SchemaStructure.whrexpressions);
		if(tempExp != null && tempExp.size() > 0) {
			Expression exp = Utils.conquerExpression(tempExp);
			iter = new SelectionIterator(iter, exp);
		}
		return iter;
	}

	public DefaultIterator pushDownJoinPredicate(List<String> leftColumns, List<String> rightColumns, 
			DefaultIterator leftIterator, DefaultIterator rightIterator, Join joinDefault) {

		DefaultIterator result = null;
		Expression exp = Optimzer.getExpressionForJoinPredicate(leftColumns, rightColumns, SchemaStructure.whrexpressions);
		if(exp != null) {
			Join join = new Join();
			join.setOnExpression(exp);
			if(Config.isInMemory) {
				result = new HashJoinIterator(leftIterator, rightIterator, join);
			}  else {
				try {
					result = new SortMergeIterator(leftIterator, rightIterator, join);
				} catch (Exception e) {
					e.printStackTrace();
				}	
			}
		} else {
			result = new JoinIterator(leftIterator, rightIterator, joinDefault); 
		}
		return result;
	}
}




