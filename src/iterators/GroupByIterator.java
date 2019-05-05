package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import utils.Accumulator;


///Considering that there is at least one expression
public class GroupByIterator implements DefaultIterator{
	List<Column> groupBy;
	Table table;
	List<SelectItem> selectItems;
	DefaultIterator iterator;
	List<String> columns;
	Map<String, Integer> columnMapper;
	List<Accumulator> accumulator;
	Map<String, Integer> expressionMap;
	int expressionPointer = 0;
	
	public GroupByIterator(DefaultIterator iterator, List<Column> groupBy, Table fromItem, List<SelectItem> selectItems) {
		this.groupBy = groupBy;
		this.table = fromItem;
		this.selectItems = selectItems;
		this.iterator = iterator;
		this.expressionMap = new HashMap<>();
		this.columns = new ArrayList<>();
		this.accumulator = new ArrayList<>();
		for(Column col: this.groupBy) {
			this.columns.add(col.getTable().getName() + "." + col.getColumnName());
		}
		
		for(int index = 0; index < this.selectItems.size();index++) {
			SelectItem selectItem = this.selectItems.get(index);
			if(selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExpression = (SelectExpressionItem) selectItem;
				if((selectExpression.getExpression() instanceof Function)){
					Function func = (Function) selectExpression.getExpression();
					String name = func.getName();
					if(selectExpression.getAlias() == null){
						if(func.getParameters() != null) {
							List<Expression> expList = func.getParameters().getExpressions();
							StringBuilder sb = new StringBuilder();
							for(Expression exp : expList) {
								sb.append(exp.toString());
							}
							this.columns.add(name + "(" + sb.toString() + ")");
							this.expressionMap.put(name + "(" + sb.toString() + ")", this.expressionPointer);
							this.expressionPointer += 1;
						} else {
							if(func.isAllColumns()) {
								this.columns.add(name + "(*)");
							}
						}
					} else {
						this.columns.add(selectExpression.getAlias());
						this.expressionMap.put(selectExpression.getAlias(), this.expressionPointer);
						this.expressionPointer += 1;
					}
					
				}	
			}	
		}
		createMapperColumn();
		createAccumulator();
	}

	private void createAccumulator() {
		for(int index = 0; index < this.selectItems.size();index++) {
			SelectItem selectItem = this.selectItems.get(index);
			SelectExpressionItem selectExpression = (SelectExpressionItem) selectItem;
			if(selectExpression.getExpression() instanceof Function) {
				Function func = (Function) selectExpression.getExpression();
				Accumulator accumulator = new Accumulator(func.getParameters() ,this.groupBy,func.getName() , this.columnMapper);
				this.accumulator.add(accumulator);
			}
		}
		while(this.iterator.hasNext()) {
			List<PrimitiveValue> map = this.iterator.next();
			for(Accumulator accumulator: this.accumulator) {
				try {
					accumulator.aggregate(map);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		for(Accumulator accumulator: this.accumulator) {
			accumulator.commit();
		}
		
	}

	private void createMapperColumn() {
		this.columnMapper = new HashMap<String, Integer>();
		int index = 0;
		for(String col: this.iterator.getColumns()) {
			this.columnMapper.put(col, index);
				index+=1;
		}
	}
	
	
	@Override
	public boolean hasNext() {
		return this.accumulator.get(0).hashNext();
	}

	@Override
	public List<PrimitiveValue> next() {
		Boolean isAddded = true;
		List<PrimitiveValue> map = new ArrayList<>();
		for(Accumulator accum: this.accumulator) {
			accum.nextHash(isAddded);
			if(isAddded) {
				map.addAll(accum.getNextList());
				isAddded = false;
			}
			try {
				map.add(accum.next());
			} catch (InvalidPrimitive e) {
				e.printStackTrace();
			}
		}
		return map;
	}

	@Override
	public void reset() {}

	@Override
	public List<String> getColumns() {
		return this.columns;
	}
}
