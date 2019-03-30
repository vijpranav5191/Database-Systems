package iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import objects.ColumnDefs;
import objects.SchemaStructure;

import java.io.*;
public class orderExternalIterator implements DefaultIterator {

	private List<OrderByElement> orderBy;
	DefaultIterator iterator;
	Table primaryTable;
	
	public orderExternalIterator(DefaultIterator iterator, List<OrderByElement> orderBy, Table primaryTable) throws IOException 
	{
		// TODO Auto-generated constructor stub
		int level = 0;
		int filenumber = 200;
		List<List<Map<String,PrimitiveValue>>> batches = new ArrayList<>();
		// branching done
		Queue<File> queue = new LinkedList<>();
		while(iterator.hasNext())
		{
			List<Map<String,PrimitiveValue>> batch = new ArrayList<Map<String,PrimitiveValue>>();
			for(int i=0;i<4 && iterator.hasNext();i++)
			{
				Map<String,PrimitiveValue> obj = iterator.next();
				batch.add(obj);
			}
//			System.out.println(primaryTable);
			List<ColumnDefs> cdef = SchemaStructure.schema.get(String.valueOf(primaryTable));
//			System.out.println(cdef);
			List<Map<String, PrimitiveValue>> result = new orderIterator().backTrack(batch, orderBy);
			Iterator<Map<String, PrimitiveValue>> itr = result.iterator();
			System.out.println("here"); 
			File filename = new File("src\\dubstep\\file\\level"+level+"_file"+filenumber+".dat");
			queue.add(filename);
			System.out.println(filename); 
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));   
			while( itr.hasNext() )
			{
				Map<String,PrimitiveValue> mp = itr.next();
				System.out.println(" " + mp);
				
				String writeInFile = "";
				
				for (ColumnDefs object : cdef)
				{
					String x = String.valueOf(primaryTable)+"."+ String.valueOf(object.cdef.getColumnName());
					System.out.println(x);
					
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
		System.out.println( );
		while(queue.size()!=1)
		{
			File one = queue.poll();
			itr1 = new TableScanIterator( primaryTable, true, one );
			
			File two = queue.poll();
			itr2 = new TableScanIterator( primaryTable, true, two);
			
			File newF = new File("src\\dubstep\\file\\level"+level+"_file"+filenumber+".dat");
			
			System.out.println( " one " +  String.valueOf(one) + " two " + String.valueOf(two) + " newF " + newF); 
			
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
			queue.add(newF);
			
			
			// makeNewFile with new Level 
		}
		// merging start
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		return this.iterator.getColumns();
	}

}
