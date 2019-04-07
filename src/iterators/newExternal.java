package iterators;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

public class newExternal implements DefaultIterator{
	DefaultIterator iterator;
	List<OrderByElement> orderBy;
	Table primaryTable ;
	List<SelectItem> selectItem;
	private List<String> mapColumn;
	private List<PrimitiveValue> pmValues;
	Map<String,PrimitiveValue> map;
	private ArrayList<PrimitiveValue> fileValues;
	DefaultIterator deItr;
	
	
	public newExternal(DefaultIterator iterator, List<OrderByElement> orderBy, Table primaryTable , List<SelectItem> selectItem) throws IOException, InvalidPrimitive, ParseException  
	{
		this.iterator = iterator;
		this.orderBy = orderBy;
		this.primaryTable = primaryTable;
		this.selectItem = selectItem;
		
//		while(this.iterator.hasNext())
//		{
//			System.out.println(this.iterator.next());
//		}
		this.mapColumn = new ArrayList<String>();
		this.pmValues = new ArrayList<PrimitiveValue>();
		
		this.fileValues = new ArrayList<PrimitiveValue>();
		int level = 0;
		int fileNumber = 1;
		
		
		
//		List<List<Map<String,PrimitiveValue>>> batches = new ArrayList<>();
		
//		
		
		Queue<File> queue = new LinkedList<>();
		boolean flag = true;
		while(this.iterator.hasNext())
		{
			
			List<Map<String,PrimitiveValue>> batch = new ArrayList<Map<String,PrimitiveValue>>();
			for(int i=0;i< 20 && this.iterator.hasNext();i++)
			{
				Map<String,PrimitiveValue> obj = iterator.next();
				
				if(flag == true)
				{    
					for(String  key : obj.keySet())
					{
						PrimitiveValue pm = obj.get(key);
//						System.out.println(mapValue);
						this.mapColumn.add(key);
						this.pmValues.add(pm);
					}
					System.out.println( mapColumn);
					flag = false;
				}
				

				batch.add(obj);
				
			}
//			System.out.println(batch);
//			
//			System.out.println( this.pmValues);
//			System.out.println( this.mapColumn);
//			System.out.println(batch);
			OrderByIterator orderList = new OrderByIterator(orderBy, batch, this.mapColumn);
			List<Map<String, PrimitiveValue>> result = new ArrayList<Map<String,PrimitiveValue>>();
			while(orderList.hasNext())
			{
				result.add(orderList.next());
			}
//			System.out.println(result);
			Iterator<Map<String, PrimitiveValue>> itr = result.iterator();
			
			File filename = new File("F:\\aa\\level_"+level+"_file"+fileNumber+".dat");
			queue.add(filename);
			
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));   
			while( itr.hasNext() )
			{
				Map<String,PrimitiveValue> mp = itr.next();
//				System.out.println(" mp " + mp);
				String writeInFile = "";
				for (String x : mapColumn)
				{
					writeInFile += String.valueOf(mp.get(x))+"|"; 
				}
				String writeInFile1 = writeInFile.substring(0, writeInFile.length()-1);
				writer.write(writeInFile1);
		        writer.newLine();
			}
			fileNumber++;
			writer.close();
		}
		
		DefaultIterator  itr1 = null;
		DefaultIterator  itr2 = null;
		level = 1;
		while(queue.size()!=1)
		{
			File one = queue.poll();
			itr1 = new fileIterator(one, pmValues, mapColumn , map);
		
			File two = queue.poll();
			itr2 = new fileIterator(two , pmValues , mapColumn , map);
			
			File newF = new File("F:\\aa\\level_"+level+"_file"+fileNumber+".dat");

//			System.out.println(" 1 " + one.toString() + " 2 " +  two.toString() + " new " + newF.toString() );
			BufferedWriter writer = new BufferedWriter(new FileWriter(newF));   
			fileNumber++;
			
			Map<String,PrimitiveValue> firstPtr = itr1.next();
			Map<String,PrimitiveValue> secondPtr = itr2.next();
			
			while( firstPtr != null && secondPtr != null )
			{
				ArrayList<Map<String,PrimitiveValue>> temp = new ArrayList<Map<String,PrimitiveValue>>(Arrays.asList(firstPtr,secondPtr));
				for( OrderByElement order : orderBy)
				{
					if(order.isAsc())
					{
						String ord = order.getExpression().toString();
						ArrayList<Map<String,PrimitiveValue>> temp1 = null;
						//						temp1 = new OrderByIterator().sortByCol(temp, 1 , ord);
						Map<String,PrimitiveValue> toWrite = new HashMap<String, PrimitiveValue>();
//						Map<String,PrimitiveValue> twoWrite = new HashMap<String, PrimitiveValue>();
						switch(firstPtr.get(ord).getType().toString().toLowerCase() )
						{
							case "int":
							case "long":
							{
//								System.out.println(" here " );
									if(firstPtr.get(ord).toLong()-secondPtr.get(ord).toLong() <=0)
									{
										toWrite = firstPtr;
										firstPtr = itr1.next();
									}
									else
									{
										toWrite = secondPtr;
										secondPtr = itr2.next();
									}
									break;
							}
							case "string":
							{
								if(firstPtr.get(ord).toString().compareTo(secondPtr.get(ord).toString()) <=0)
								{
									toWrite = firstPtr;
									firstPtr = itr1.next();
								}
								else
								{
									toWrite = secondPtr;
									secondPtr = itr2.next();
								}
								break;	
							}
							case "double":
							{
								if(firstPtr.get(ord).toDouble()-(secondPtr.get(ord).toDouble()) <=0)
								{
									toWrite = firstPtr;
									firstPtr = itr1.next();
								}
								else
								{
									toWrite = secondPtr;
									secondPtr = itr2.next();
								}
								break;
							}
							case "date":
								SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
								Date dateFirst = (Date) sdf.parse( String.valueOf(firstPtr.get(ord)) );
								Date dateSecond = (Date) sdf.parse( String.valueOf(secondPtr.get(ord)) );
								if(dateFirst.compareTo(dateSecond) <= 0)
								{
									toWrite = firstPtr;
									firstPtr = itr1.next();
								}
								else
								{
									toWrite = secondPtr;
									secondPtr = itr2.next();
								}	
								break;
							default:
								if(firstPtr.get(ord).toString().compareTo(secondPtr.get(ord).toString()) >=0)
								{
									toWrite = firstPtr;
									firstPtr = itr1.next();
								}
								else
								{
									toWrite = secondPtr;
									secondPtr = itr2.next();
								}
								break;	
						}
						
						String writeA = "";
						for (String object : mapColumn)
						{
					         writeA += (toWrite.get(object) + "|").toString() ;
					    }
						writeA = writeA.substring(0,writeA.length()-1);
						writer.write(writeA);
						writer.newLine();							
					
					}
					else
					{
						String ord = order.getExpression().toString();
						ArrayList<Map<String,PrimitiveValue>> temp1 = null;
						Map<String,PrimitiveValue> toWrite = new HashMap<String, PrimitiveValue>();
						switch(firstPtr.get(ord).getType().toString().toLowerCase() )
						{
							case "int":
							case "long":
							{
//								System.out.println(" here " );
									if(firstPtr.get(ord).toLong()-secondPtr.get(ord).toLong() <=0)
									{
										toWrite = secondPtr;
										secondPtr = itr2.next();
									}
									else
									{
										toWrite = firstPtr;
										firstPtr = itr1.next();
									}
									break;
							}
							case "string":
							{
								if(firstPtr.get(ord).toString().compareTo(secondPtr.get(ord).toString()) <=0)
								{
									toWrite = secondPtr;
									secondPtr = itr2.next();
								}
								else
								{
									toWrite = firstPtr;
									firstPtr = itr1.next();
								}
								break;	
							}
							case "double":
							{
								if(firstPtr.get(ord).toDouble()-(secondPtr.get(ord).toDouble()) <=0)
								{
									toWrite = secondPtr;
									secondPtr = itr2.next();
								}
								else
								{
									toWrite = firstPtr;
									firstPtr = itr1.next();
								}
								break;
							}
							case "date":
								SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
								Date dateFirst = sdf.parse( String.valueOf(firstPtr.get(ord)) );
								Date dateSecond = sdf.parse( String.valueOf(secondPtr.get(ord)) );
								if(dateFirst.compareTo(dateSecond) <= 0)
								{
									toWrite = secondPtr;
									secondPtr = itr2.next();
								}
								else
								{
									toWrite = firstPtr;
									firstPtr = itr1.next();
								}
								break;
							default:
								if(firstPtr.get(ord).toString().compareTo(secondPtr.get(ord).toString()) >=0)
								{
									toWrite = secondPtr;
									secondPtr = itr2.next();
								}
								else
								{
									toWrite = firstPtr;
									firstPtr = itr1.next();
								}
								break;	
						}
						
						String writeA = "";
						for (String object : mapColumn)
						{
					         writeA += (toWrite.get(object) + "|").toString() ;
					    }
						writeA = writeA.substring(0,writeA.length()-1);
						writer.write(writeA);
						writer.newLine();	
					
					}
				}	
			}
			while(firstPtr != null && secondPtr == null)
			{
				String x = "";
				for (String object : mapColumn)
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
				for (String object : mapColumn)
				{
//		            writer.write(String.valueOf(firstPtr.get(object)+"|"));
				 	x += (String.valueOf(secondPtr.get(object)+"|"));
		        }
				x = x.substring(0,x.length()-1);
				writer.write(x);
		        writer.newLine();
		        secondPtr = itr2.next();
			}
			writer.close();
			
			queue.add(newF);

		}
		File fileName = queue.poll();
		String fr = String.valueOf(fileName);		
		this.deItr = new fileIterator(fr , selectItem , mapColumn , pmValues);
		
		
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return this.deItr.hasNext();
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return this.deItr.next();
	}

	@Override
	public void reset() {
		this.deItr.reset();// TODO Auto-generated method stub
		
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
