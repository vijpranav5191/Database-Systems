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

import javax.sound.midi.Soundbank;

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
	String str;
	
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
			for(int i=0;i< 5000 && this.iterator.hasNext();i++)
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
//					System.out.println( mapColumn);
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
			
			File filename = new File("F:\\ff\\level_"+level+"_file"+fileNumber+".dat");
			queue.add(filename);
			
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));   
			while( itr.hasNext() )
			{
				Map<String,PrimitiveValue> mp = itr.next();
//				System.out.println(" mp " + mp);
				String writeInFile = "";
				for (String x : mapColumn)
				{
					writeInFile += ((mp.get(x))+"|").toString(); 
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
		Map<String,PrimitiveValue> firstPtr = null;;
		Map<String,PrimitiveValue> secondPtr = null;
		while(queue.size()!=1)
		{
			File one = queue.poll();
			itr1 = new fileIterator(one , pmValues , mapColumn , map);
			
			File two = queue.poll();
			itr2 = new fileIterator(two , pmValues , mapColumn, map);
			this.str = "F:\\ff\\level_"+level+"_file"+fileNumber+".dat";
			

			File newF = new File("F:\\ff\\level_"+level+"_file"+fileNumber+".dat");
//			System.out.println( " 1 " +  one.toString() + " 2 " +two.toString() + " 3 "+ newF.toString());
			BufferedWriter writer = new BufferedWriter(new FileWriter(newF));   
			fileNumber++;
			firstPtr = itr1.next();
//			System.out.println(" FIRST PTR " + firstPtr);
			secondPtr = itr2.next();
//			System.out.println(" Second PTR " + firstPtr);
			//			if(upDatescolmnValues != null)
//				this.upDatescolmnValues.clear();
//			for(String key : firstPtr.keySet())
//			{
//				System.out.println("here");
//				this.upDatescolmnValues.add(key);
//			}
			while(firstPtr != null && secondPtr != null)
			{
				boolean flagInserted = false;
				Map<String , PrimitiveValue> toWrite = new HashMap<String, PrimitiveValue>();
				for (OrderByElement order : orderBy)
				{
					String ord = order.getExpression().toString();
					if(order.isAsc())
					{
//						System.out.println(firstPtr);
//						System.out.println(firstPtr.get(ord).getType().toString().toLowerCase());
						if(firstPtr.get(ord).getType().toString().toLowerCase().equals("int"))
						{
//							System.out.println(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong());
							if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() == 0)
							{
//								System.out.println( " COMMON ");
								continue;
							}
							else if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() < 0)
							{
//								System.out.println( " HERE ");
								
								writeToFile(firstPtr , writer);
								firstPtr = itr1.next();
								flagInserted = true;
								break;
							}
							else if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() > 0)
							{
								writeToFile(secondPtr, writer);
								secondPtr = itr2.next();
								flagInserted = true;
								break;
							}
						}
						else if(firstPtr.get(ord).getType().toString().toLowerCase().equals("long"))
						{
//							System.out.println(firstPtr.get(ord).toLong() + " " +  secondPtr.get(ord).toLong());
							
							if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() == 0)
							{
//								System.out.println(" common ");
								continue;
							}
							else if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() < 0)
							{
//								System.out.println(" LONG INN ");
								writeToFile(firstPtr , writer);
								firstPtr = itr1.next();
								flagInserted = true;
								break;
							}
							else if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() > 0)
							{
								writeToFile(secondPtr, writer);
								secondPtr = itr2.next();
								flagInserted = true;
								break;
							}
							
						}
						else if(firstPtr.get(ord).getType().toString().toLowerCase().equals("date"))
						{
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							Date dateFirst = sdf.parse(firstPtr.get(ord).toString() );
						    Date dateSecond = sdf.parse(secondPtr.get(ord).toString());
	
							if(dateFirst.compareTo(dateSecond) == 0)
							{
								continue;
							}
							else if(dateFirst.compareTo(dateSecond) < 0)
							{
								writeToFile(firstPtr , writer);
								firstPtr = itr1.next();	
								flagInserted = true;
								break;
							}
							else if(dateFirst.compareTo(dateSecond) > 0)
							{
								writeToFile(secondPtr, writer);
								secondPtr = itr2.next();
								flagInserted = true;
								break;
							}
							
							
						}
						else if(firstPtr.get(ord).getType().toString().toLowerCase().equals("double"))
						{
							if(firstPtr.get(ord).toDouble() - secondPtr.get(ord).toDouble() == 0)
							{
								continue;
							}
							else if(firstPtr.get(ord).toDouble() - secondPtr.get(ord).toDouble() < 0)
							{
								writeToFile(firstPtr , writer);
								firstPtr = itr1.next();	
								flagInserted = true;
								break;
							}
							else if(firstPtr.get(ord).toDouble() - secondPtr.get(ord).toDouble() > 0)
							{
								writeToFile(secondPtr, writer);
								secondPtr = itr2.next();	
								flagInserted = true;
								break;
							}
						}
						else if(firstPtr.get(ord).getType().toString().toLowerCase().equals("string"))
						{
							if(firstPtr.get(ord).toString().compareTo(secondPtr.get(ord).toString()) ==0)
							{
								continue;
							}
							else if(firstPtr.get(ord).toString().compareTo(secondPtr.get(ord).toString()) < 0)
							{
								writeToFile(firstPtr , writer);
								firstPtr = itr1.next();	
								flagInserted = true;
								break;
							}
							else if(firstPtr.get(ord).toString().compareTo(secondPtr.get(ord).toString()) > 0)
							{
								writeToFile(secondPtr , writer);
								secondPtr = itr2.next();	
								flagInserted = true;
								break;
							}
						}
					}
					else
					{
						
						if(firstPtr.get(ord).getType().toString().toLowerCase().equals("int"))
						{
							if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() == 0)
							{
								
								continue;
							}
							else if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() < 0)
							{
								writeToFile(secondPtr, writer);
								secondPtr = itr2.next();
								flagInserted = true;
								break;
							}
							else if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() > 0)
							{
								writeToFile(firstPtr , writer);
								firstPtr = itr1.next();
								flagInserted = true;
								break;
							}
						}
						else if(firstPtr.get(ord).getType().toString().toLowerCase().equals("long"))
						{
							if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() == 0)
							{
								continue;
							}
							else if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() < 0)
							{
								writeToFile(secondPtr, writer);
								secondPtr = itr2.next();
								flagInserted = true;
								break;
							}
							else if(firstPtr.get(ord).toLong() - secondPtr.get(ord).toLong() > 0)
							{
								writeToFile(firstPtr , writer);
								firstPtr = itr1.next();
								flagInserted = true;
								break;
							}
							
						}
						else if(firstPtr.get(ord).getType().toString().toLowerCase().equals("date"))
						{
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							Date dateFirst = sdf.parse(firstPtr.get(ord).toString() );
						    Date dateSecond = sdf.parse(secondPtr.get(ord).toString());
	
							if(dateFirst.compareTo(dateSecond) == 0)
							{
								continue;
							}
							else if(dateFirst.compareTo(dateSecond) < 0)
							{
								writeToFile(secondPtr , writer);
								secondPtr = itr2.next();
								flagInserted = true;
								break;
							}
							else if(dateFirst.compareTo(dateSecond) > 0)
							{
								writeToFile(firstPtr , writer);
								firstPtr = itr1.next();	
								flagInserted = true;
								break;
							}
						}
						else if(firstPtr.get(ord).getType().toString().toLowerCase().equals("double"))
						{
							if(firstPtr.get(ord).toDouble() - secondPtr.get(ord).toDouble() == 0)
							{
								continue;
							}
							else if(firstPtr.get(ord).toDouble() - secondPtr.get(ord).toDouble() < 0)
							{
								writeToFile(secondPtr, writer);
								secondPtr = itr2.next();
								flagInserted = true;
								break;
							}
							else if(firstPtr.get(ord).toDouble() - secondPtr.get(ord).toDouble() > 0)
							{	
								writeToFile(firstPtr , writer);
								firstPtr = itr1.next();
								flagInserted = true;
								break;
							}
						}
						else if(firstPtr.get(ord).getType().toString().toLowerCase().equals("string"))
						{
							if(firstPtr.get(ord).toString().compareTo(secondPtr.get(ord).toString()) ==0)
							{
								continue;
							}
							else if(firstPtr.get(ord).toString().compareTo(secondPtr.get(ord).toString()) < 0)
							{
								writeToFile(secondPtr , writer);
								secondPtr = itr2.next();
								flagInserted = true;
								break;
							}
							else if(firstPtr.get(ord).toString().compareTo(secondPtr.get(ord).toString()) > 0)
							{	
								writeToFile(firstPtr , writer);
								firstPtr = itr1.next();	
								flagInserted = true;
								break;
							}
						}
						
						
					}						
				}
				if(!flagInserted)
				{
					writeToFile(firstPtr , writer);
					firstPtr = itr1.next();
					writeToFile(secondPtr, writer);
					secondPtr = itr2.next();
				}
			}
			while(firstPtr != null && secondPtr == null)
			{
				writeToFile(firstPtr, writer);
		        firstPtr = itr1.next();
			}
			while(firstPtr == null && secondPtr != null)
			{
				writeToFile(secondPtr , writer);
		        secondPtr = itr2.next();
			}
			writer.close();
			queue.add(newF);
		}
		
		File fileName = queue.poll();
		String fr = String.valueOf(fileName);		
		this.deItr = new fileIterator(fr , selectItem , mapColumn , pmValues);
		
		
	}
	private void writeToFile(Map<String, PrimitiveValue> toWrite  , BufferedWriter writer) throws IOException {
		// TODO Auto-generated method stub
//		System.out.println(" " + toWrite);
		String writeA = "";
		for (String object : mapColumn)
		{
	         writeA += (toWrite.get(object) + "|").toString() ;
	    }
		writeA = writeA.substring(0,writeA.length()-1);
		writer.write(writeA);
		writer.newLine();
		
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
