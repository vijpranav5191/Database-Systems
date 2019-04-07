package iterators;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import utils.Config;

public class groupByExternal implements DefaultIterator {
	DefaultIterator iterator;
	DefaultIterator deItr;
	List<Column> groupBy;
	Table primaryTable;
	List<SelectItem> selectItems;
	private List<String> colmnValues;
	private List<PrimitiveValue> pmValues;
	String str;
	
	public groupByExternal(DefaultIterator iterator,List<Column> groupBy , Table primaryTable , List<SelectItem> selectItems) throws Exception {
		// TODO Auto-generated constructor stub
		this.iterator = iterator;
		this.groupBy = groupBy;
		this.primaryTable = primaryTable;
		this.selectItems = selectItems;
		
		List<OrderByElement> ordElem =  new ArrayList<OrderByElement>();
		for(Column col : groupBy)
		{
			OrderByElement ord = new OrderByElement();
			ord.setExpression(col);
			
			ordElem.add( ord);
			
		}
		
//		System.out.println( " columnValue " + column);
		int level = 0;
 		int filenumber = 1;
		
 		List<List<Map<String,PrimitiveValue>>> batches = new ArrayList<>();
		// branching done
//		DefaultIterator abc = iterator;
		colmnValues = new ArrayList<String>();
		pmValues = new ArrayList<PrimitiveValue>();
		
		Map<String ,PrimitiveValue> mapValue = this.iterator.next();
		this.iterator.reset();

		for(String  key : mapValue.keySet())
		{
			PrimitiveValue pm = mapValue.get(key);
//			System.out.println(key);
			colmnValues.add(key);
			pmValues.add(pm);
		}
		
		Queue<File> queue = new LinkedList<>();
		while(iterator.hasNext())
		{
			List<Map<String,PrimitiveValue>> batch = new ArrayList<Map<String,PrimitiveValue>>();


			for(int i=0;i<Config.blockSize && iterator.hasNext();i++)
			{
				Map<String,PrimitiveValue> obj = iterator.next();
				mapValue = obj;
				batch.add(obj);
			}
//			Iterator<Map<String, PrimitiveValue>> itr = batch.iterator();
//			OrderByIterator orderList = new OrderByIterator(ordElem, iterator);
//			
			OrderByIterator orderList = new OrderByIterator(ordElem, batch, colmnValues);
			
			List<Map<String, PrimitiveValue>> result = new ArrayList<Map<String,PrimitiveValue>>();
			while(orderList.hasNext())
			{
				result.add(orderList.next());
			}
			Iterator<Map<String, PrimitiveValue>> itr = result.iterator();
			File filename = new File("F:\\ff2\\level"+level+"_file"+filenumber+".dat");
//=======
//
//			List<List<Map<String, PrimitiveValue>>> result = new GroupByIterator().backTrack(batch, groupBy);
//			System.out.println(result);
//			Iterator<List<Map<String, PrimitiveValue>>> itr = result.iterator();
////			System.out.println("here"); 
//			File filename = new File("D:\\temp\\"+level+"_file"+filenumber+".dat");
//>>>>>>> d055e9d00d16c70de8e1b6691c6eee8aac9cdd0e
			queue.add(filename);
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));   
			while( itr.hasNext() )
			{
				Map<String,PrimitiveValue> listMap = itr.next();
					String writeInFile = "";	
					for (String x : itr.next().keySet() )
					{
						writeInFile += x+"|"; 
					}
//					System.out.println( " writeFile " +  writeInFile );
					String writeInFile1 = writeInFile.substring(0, writeInFile.length()-1);
					writer.write(writeInFile1);
					writer.newLine();
			}
			filenumber++;
			writer.close();
		}
		DefaultIterator  itr1 = null;
		DefaultIterator  itr2 = null;
		while(queue.size()!=1)
		{
			File one = queue.poll();
			itr1 = new fileIterator(one , pmValues , colmnValues , mapValue);
			
			File two = queue.poll();
			itr2 = new fileIterator(two , pmValues , colmnValues , mapValue);
			
			this.str = "F:\\ff2\\level"+level+"_file"+filenumber+".dat";
			File newF = new File("F:\\ff2\\level"+level+"_file"+filenumber+".dat");
			
			BufferedWriter writer = new BufferedWriter(new FileWriter(newF));   
			filenumber++;
			Map<String,PrimitiveValue> firstPtr = itr1.next();
			Map<String,PrimitiveValue> secondPtr = itr2.next();
			
			while(firstPtr != null && secondPtr != null)
			{
				
				boolean insertedFlag = false;
				for( Column orderElem : groupBy )
				{
					if(String.valueOf(firstPtr.get(String.valueOf(orderElem))).compareTo(String.valueOf(secondPtr.get(String.valueOf(orderElem)))) == 0)
					{
						continue;
					}
					else if(String.valueOf(firstPtr.get(String.valueOf(orderElem))).compareTo(String.valueOf(secondPtr.get(String.valueOf(orderElem)))) > 0)
					{
//						Map<String,PrimitiveValue> mp = firstPtr;
//						System.out.println(" " + mp);
						String writeB = "";
						for (String object : secondPtr.keySet())
						{
				         	writeB += String.valueOf(secondPtr.get(object) + "|") ;
				        }
						writeB = writeB.substring(0,writeB.length()-1);
						writer.write(writeB);
						writer.newLine();
				        secondPtr = itr2.next();
				        insertedFlag = true;
				        break;
					}
					else if(String.valueOf(firstPtr.get(String.valueOf(orderElem))).compareTo(String.valueOf(secondPtr.get(String.valueOf(orderElem)))) < 0)
					{
						String writeA = "";
						for (String object : firstPtr.keySet())
						{
//				            writer.write(String.valueOf(firstPtr.get(object)+"|"));
							writeA += (String.valueOf(firstPtr.get(object)+"|"));
				        }
						writeA = writeA.substring(0,writeA.length()-1);
						writer.write(writeA);
				        writer.newLine();
				        firstPtr = itr1.next();
				        insertedFlag = true;
				        break;
					}
				}
				if(!insertedFlag)
				{
					
					String writeEA = "";
					
					for (String object : firstPtr.keySet())
					{
			    		writeEA += (String.valueOf(firstPtr.get(object)+"|"));
			        }
					writeEA = writeEA.substring(0,writeEA.length()-1);
					writer.write(writeEA);
			        writer.newLine();
					
					String writeEB = "";
			        for (String object : secondPtr.keySet())
					{
			        	writeEB += (String.valueOf(secondPtr.get(object)+"|"));
			        }
					writeEB = writeEB.substring(0,writeEB.length()-1);
					writer.write(writeEB);
			        writer.newLine();
			        firstPtr = itr1.next();
			        secondPtr = itr2.next();
				}	
			}
			while(firstPtr != null && secondPtr == null)
			{
				String x = "";
				for (String object : firstPtr.keySet())
				{
//		            writer.write(String.valueOf(firstPtr.get(object)+"|"));
				 	x += (String.valueOf(firstPtr.get(object)+"|"));
		        }
				x = x.substring(0,x.length()-1);
				writer.write(x);
		        writer.newLine();
		        firstPtr = itr1.next();
			}
			while(firstPtr == null && secondPtr != null)
			{
				String x = "";
				for (String object : secondPtr.keySet())
				{
//		            writer.write(String.valueOf(firstPtr.get(object)+"|"));
				 	x += (String.valueOf(secondPtr.get(object)+"|"));
		        }
				x = x.substring(0,x.length()-1);
				writer.write(x);
		        writer.newLine();
		        secondPtr = itr2.next();
			}
//			System.out.println(" ithe " + itr1.next() );
//			System.out.println(" ithe " + itr2.next() );
			writer.close();
//			one.delete();
//			two.delete();
			queue.add(newF);
			
			
			// makeNewFile with new Level 
		}
		
		File fileName = queue.poll();
		String fr = String.valueOf(fileName);		
		deItr = new fileIterator(fr , selectItems , colmnValues , pmValues);
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return this.deItr.hasNext();
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		Map<String, PrimitiveValue> selectMap = new HashMap<String, PrimitiveValue>();
		
		if(this.deItr.hasNext())
		{
			Map<String, PrimitiveValue> pm = this.deItr.next();
			ArrayList<Map<String, PrimitiveValue>> group = getArrayList( this.deItr , pm , groupBy);
			Iterator iter = group.iterator();
			Map<String, PrimitiveValue> map = (Map<String, PrimitiveValue>) iter.next();
			if(map!=null) {
				for(int index = 0; index < this.selectItems.size();index++) {
					SelectItem selectItem = this.selectItems.get(index);
					
					if(selectItem instanceof AllTableColumns) {
						AllTableColumns allTableColumns = (AllTableColumns) selectItem;
						allTableColumns.getTable();
						selectMap = map;
					} else if(selectItem instanceof AllColumns) {
						AllColumns allColumns = (AllColumns) selectItem;	
						selectMap = map;
					} else if(selectItem instanceof SelectExpressionItem) {
						SelectExpressionItem selectExpression = (SelectExpressionItem) selectItem;
						if(selectExpression.getExpression() instanceof Column) {
							Column column = (Column) selectExpression.getExpression();
							if(column.getTable().getName() != null && column.getColumnName() != null) {
								selectMap.put(column.getTable().getName() + "." + column.getColumnName(), map.get(column.getTable().getName() + "." + column.getColumnName()));
							} else if(column.getTable().getAlias() != null && column.getColumnName() != null) {
								selectMap.put(column.getTable().getAlias() + "." + column.getColumnName(), map.get(column.getTable().getAlias() + "." + column.getColumnName()));		
							} else if(column.getTable().getAlias() == null && column.getTable().getName() == null){
								for(String key: map.keySet()) {
									if(key.split("\\.")[1].equals(column.getColumnName())) {
										selectMap.put(key, map.get(key));					
										break;
									}
								}
							}
						} else if(selectExpression.getExpression() instanceof Function) {
							try {
								Expression exp = selectExpression.getExpression();
								if(exp instanceof Function) {
									Function func = (Function) exp;
									iter = group.iterator();
									DefaultIterator iter1 = new SimpleAggregateIterator(iter, func);
									selectMap.putAll(iter1.next());	
								}
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
			}
			
		}
		return selectMap;
		
	}

	private ArrayList<Map<String, PrimitiveValue>> getArrayList(DefaultIterator itr,
			Map<String, PrimitiveValue> pm , List<Column> groupBy) {
		// TODO Auto-generated method stub
		ArrayList<Map<String, PrimitiveValue>> resultList = new ArrayList<Map<String,PrimitiveValue>>();
		resultList.add( pm );
		while(itr.hasNext())
		{
			Map<String, PrimitiveValue> pmNew = itr.next();  
			for(Column group : groupBy)
			{
				if( !pmNew.get(group.toString()).equals(pm.get(group.toString())) )
				{
					this.deItr = itr;
					return resultList;
				}	
			}
			resultList.add(pmNew);
		}
		return resultList;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.deItr.reset();
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		return this.deItr.getColumns();
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return this.iterator;
	}

	
}
