package iterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;
import objects.ColumnDefs;
import objects.SchemaStructure;
import utils.Config;

import java.io.*;
import java.nio.file.Files;
import java.sql.ResultSet;
public class orderExternalIterator implements DefaultIterator {

	private List<OrderByElement> orderBy;
	
	Map<String, PrimitiveValue> mapValue;
	BufferedReader br;
	DefaultIterator iterator;
	Table primaryTable;
	String sCurrentString;
	DefaultIterator deItr;
	DefaultIterator dummy;
	private List<String> colmnValues;
	private List<SelectItem> column;
	private List<PrimitiveValue> pmValues;
	String str;
	public orderExternalIterator(DefaultIterator iterator, List<OrderByElement> orderBy, Table primaryTable , List<SelectItem> column) throws Exception 
	{
		// TODO Auto-generated constructor stub
		this.iterator = iterator;
		this.orderBy = orderBy;
		this.column = column;
		
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
			for(int i=0;i< Config.blockSize && iterator.hasNext();i++)
			{
				Map<String,PrimitiveValue> obj = iterator.next();
				mapValue = obj;
				batch.add(obj);
			}
//			System.out.println(primaryTable);
//			List<ColumnDefs> cdef = SchemaStructure.schema.get(String.valueOf(primaryTable));
//			System.out.println(cdef);
			List<Map<String, PrimitiveValue>> result = new orderIterator().backTrack(batch, orderBy);
			Iterator<Map<String, PrimitiveValue>> itr = result.iterator();
//			System.out.println("here"); 
			File filename = new File("D:\\temp\\"+level+"_file"+filenumber+".dat");
			queue.add(filename);
//			System.out.println(filename); 
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));   
			while( itr.hasNext() )
			{
				Map<String,PrimitiveValue> mp = itr.next();
//				System.out.println(" " + mp);
				
				
				String writeInFile = "";
				
				for (String x : mp.keySet())
				{
//					String x = String.valueOf(primaryTable)+"."+ String.valueOf(object.cdef.getColumnName());
//					System.out.println(x);
					
//		            writer.write(String.valueOf(mp.get(object)+"|"));
//					System.out.println( " " + object);
					writeInFile += String.valueOf(mp.get(x))+"|"; 
				}
				String writeInFile1 = writeInFile.substring(0, writeInFile.length()-1);
				writer.write(writeInFile1);
		        writer.newLine();
				// write to file;     
			}
			filenumber++;
			writer.close();
//			filename.close();
		}
		DefaultIterator  itr1 = null;
		DefaultIterator  itr2 = null;
		level = 1; 
//		System.out.println( );
		while(queue.size()!=1)
		{
			File one = queue.poll();
			itr1 = new fileIterator(one , pmValues , colmnValues);
			
			File two = queue.poll();
			itr2 = new fileIterator(two , pmValues , colmnValues);
			
			str = "D:\\temp\\"+level+"_file"+filenumber+".dat";
			File newF = new File("D:\\temp"+level+"_file"+filenumber+".dat");
			
			BufferedWriter writer = new BufferedWriter(new FileWriter(newF));   
			filenumber++;
			Map<String,PrimitiveValue> firstPtr = itr1.next();
			Map<String,PrimitiveValue> secondPtr = itr2.next();
			
			while(firstPtr != null && secondPtr != null)
			{
				
				boolean insertedFlag = false;
				for( OrderByElement orderElem : orderBy )
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
		
		// merging start
		File fileName = queue.poll();
		String fr = String.valueOf(fileName);		
		deItr = new fileIterator(fr , column , colmnValues , pmValues);

		 
	}
	@Override
	public boolean hasNext() 
		// TODO Auto-generated method stub
	{
		return this.deItr.hasNext();
	}
//		return false;
	

	@Override
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return this.deItr.next();
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.deItr.reset();//
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
