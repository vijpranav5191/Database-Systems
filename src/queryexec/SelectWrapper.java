package queryexec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import bPlusTree.BPlusTreeBuilder;
import iterators.DefaultIterator;
import iterators.HavingIterator;
import iterators.IndexJoinIterator;
import iterators.IndexSelectionIterator;
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
import iterators.newExternal;
import iterators.newGroupBy;
import iterators.newGroupByExternal;
import iterators.orderExternalIterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import objects.ColumnDefs;
import objects.SchemaStructure;
import utils.Config;
import utils.Optimzer;
import utils.Utils;

public class SelectWrapper {
	private PlainSelect plainselect;
	private List<SelectItem> selectItems;
	private Expression whereExp;
	private List<Column> groupBy;
	private List<Join> joins;
	private List<OrderByElement> orderBy;

	private Limit limit;
	private Expression having;

	public SelectWrapper(PlainSelect plainselect) {
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

		// Expression exp =
		// Optimzer.getExpressionForJoinPredicate(SchemaStructure.tableMap.get(leftTable),
		// SchemaStructure.schema.get(leftTable),
		// SchemaStructure.tableMap.get(rightTable),
		// SchemaStructure.schema.get(rightTable), SchemaStructure.whrexpressions);
		// System.out.print(exp);
		//
		// List<Expression> temp =
		// Optimzer.getExpressionForSelectionPredicate(SchemaStructure.tableMap.get(rightTable),
		// SchemaStructure.schema.get(rightTable), SchemaStructure.whrexpressions);
		// System.out.print(temp);

		if (fromItem instanceof Table) {
			Table table = (Table) fromItem;
			iter = new TableScanIterator(table);
			iter = pushDownSelectPredicate(table, iter);
		}

		DefaultIterator result = iter;

		while (result.hasNext()) {

			if ((this.joins = this.plainselect.getJoins()) != null) {
				for (Join join : joins) {
					FromItem item = join.getRightItem();
					if (item instanceof Table) {
						Table rightTb = (Table) item;
						DefaultIterator iter2 = new TableScanIterator(rightTb);
						iter2 = pushDownSelectPredicate(rightTb, iter2);
						List<String> leftColumns = result.getColumns();
						List<String> rightColumns = iter2.getColumns();

						result = pushDownJoinPredicate(leftColumns, rightColumns, result, iter2, join);
					}
				}
			}

			if (this.whereExp != null) 
			{
				
				List<Expression> tempExp = SchemaStructure.whrexpressions;
				HashMap< String , List<Index>> indexMap = SchemaStructure.indexMap;
				
				if (tempExp != null && tempExp.size() > 0) 
				{
					
					for(Expression itrExp : tempExp)
					{
						if( itrExp instanceof GreaterThan   )
						{
							GreaterThan expGreaterThan = (GreaterThan) itrExp;
							Column leftExp = (Column)expGreaterThan.getLeftExpression();
							Column rightExp = (Column)expGreaterThan.getRightExpression();
							String leftColumnName = null;
							Table leftTableName = null;
							if( leftExp instanceof Column)
							{
								leftColumnName =   leftExp.getColumnName(); 
								leftTableName =  leftExp.getTable(); 
							}
							if(leftColumnName != null ) 
								function( result ,  indexMap , leftColumnName , leftTableName  );			
							
						}
						
						
					}
					
//					Expression exp = Utils.conquerExpression(tempExp);
//					result = new SelectionIterator(result, exp);
				}
				
			}

			if (this.groupBy != null) {
				for (Column key : groupBy) {
					String xKey = key.getColumnName();
					if (key.getTable() == null) {
						key.setTable(SchemaStructure.columnTableMap.getOrDefault(xKey, (Table) fromItem));
					}
				}
				if (Config.isInMemory) {
					result = new newGroupBy(result, this.groupBy, (Table) fromItem, this.selectItems);
				} else {
					result = new newGroupByExternal(result, this.groupBy, (Table) fromItem, this.selectItems);
				}
			}

			if (this.having != null) {
				result = new HavingIterator(result, this.having, this.selectItems);
			}

			if (this.selectItems != null) {
				result = new ProjectionIterator(result, this.selectItems, (Table) fromItem, this.groupBy);
			}

			if (this.orderBy != null) {
				for (OrderByElement key : this.orderBy) {
					String xKey = key.getExpression().toString();
					if (xKey.split("\\.").length == 1) {
						Table defTable = (Table) fromItem;
						Column cCol = new Column();
						if (isContainingColumn(xKey, SchemaStructure.schema.get(defTable.getName()))) {
							cCol.setColumnName(xKey);
							cCol.setTable(defTable);
						} else {
							// case for alias
							cCol.setColumnName(xKey);
							cCol.setTable(SchemaStructure.columnTableMap.getOrDefault(xKey, null));
						}
						key.setExpression(cCol);
					}
				}
				if (Config.isInMemory) {
					result = new OrderByIterator(this.orderBy, result);
				} else {
					result = new newExternal(result, this.orderBy, this.selectItems);
				}
			}

			if (this.limit != null) {
				result = new LimitIterator(result, this.limit);
			}

			ResultIterator res = new ResultIterator(result);
			while (res.hasNext()) {
				res.next();
			}
		}
	}

	private void function(DefaultIterator iterator , HashMap<String, List<Index>> indexMap, String columnName, Table tableName) {
		// TODO Auto-generated method stub
		List<Index> indexes =  indexMap.get( tableName.toString());
		
		if( indexes.contains(columnName))
		{
			iterator = new IndexSelectionIterator(iterator , tableName , columnName);
		}
		else
		{
			iterator = new IndexSelectionIterator(iterator , tableName , columnName);
		}
	}

	private boolean isContainingColumn(String xKey, List<ColumnDefs> list) {
		for (ColumnDefs cdef : list) {
			if (cdef.cdef.getColumnName().equals(xKey)) {
				return true;
			}
		}
		return false;
	}

	public DefaultIterator optimize(DefaultIterator root) {

		if (root instanceof SelectionIterator) {
			SelectionIterator selIter = (SelectionIterator) root;
			if (selIter.getChildIter() instanceof JoinIterator) {
				DefaultIterator joiniter = (JoinIterator) selIter.getChildIter();
				Expression selExp = selIter.getWhereExp();
			}
		}
		return null;
	}

	public List<Expression> splitAndClauses(Expression e) {
		List<Expression> ret = new ArrayList<>();
		if (e instanceof AndExpression) {
			AndExpression a = (AndExpression) e;
			ret.addAll(splitAndClauses(a.getLeftExpression()));
			ret.addAll(splitAndClauses(a.getRightExpression()));
		} else {
			ret.add(e);
		}
		return null;
	}

	public DefaultIterator pushDownSelectPredicate(Table table, DefaultIterator iter) {
		List<Expression> tempExp = Optimzer.getExpressionForSelectionPredicate(table,
				SchemaStructure.schema.get(table.getName()), SchemaStructure.whrexpressions);
		if (tempExp != null && tempExp.size() > 0) {
			Expression exp = Utils.conquerExpression(tempExp);
			iter = new SelectionIterator(iter, exp);
		}
		return iter;
	}

	public DefaultIterator pushDownJoinPredicate(List<String> leftColumns, List<String> rightColumns,
			DefaultIterator leftIterator, DefaultIterator rightIterator, Join joinDefault) {

		DefaultIterator result = null;
		Expression exp = Optimzer.getExpressionForJoinPredicate(leftColumns, rightColumns,
				SchemaStructure.whrexpressions);
		if (exp != null) {
			Join join = new Join();
			join.setOnExpression(exp);

			if (Config.isInMemory) {
				if(exp instanceof EqualsTo) {
					EqualsTo eqexp = (EqualsTo) exp;
					Expression leftEx = eqexp.getLeftExpression();
					Expression rightEx = eqexp.getRightExpression();
					if(leftEx instanceof Column) {
						Column left = (Column) leftEx;
						if(isIndexed(left.getTable(),left.getColumnName())) {
							result = new IndexJoinIterator(rightIterator, leftIterator, join, left, (Column)rightEx);
						}
					}else if(rightEx instanceof Column) {
						Column right = (Column) rightEx;
						if(isIndexed(right.getTable(),right.getColumnName())) {
							result = new IndexJoinIterator(leftIterator,rightIterator, join, right, (Column)leftEx);
						}
					}
				}else {
					result = new HashJoinIterator(leftIterator, rightIterator, join);
				}
			} else {
				try {
					result = new SortMergeIterator(leftIterator, rightIterator, join, this.selectItems);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} else {
			result = new JoinIterator(leftIterator, rightIterator, joinDefault);
		}
		return result;
	}

	private boolean isIndexed(Table table, String columnName) {
		// TODO Auto-generated method stub
		List<Index> indexes = SchemaStructure.indexMap.get(table.getName());
		for(Index index : indexes) {
			List<String> colnames =  index.getColumnsNames();
			for(String name : colnames) {
				if(name.equals(columnName))
					return true;
			}
		}
		return false;
	}
}
