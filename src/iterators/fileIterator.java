package iterators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;



import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Arrays;
import java.util.HashMap;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
//import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;
//import objects.ColumnDefs;
//import objects.SchemaStructure;

public class fileIterator implements DefaultIterator {
	
	private String csvFile;
	List<PrimitiveValue> pm;
//	private String tableName;
	private BufferedReader br;
	private String tuple;
	private List<SelectItem> selectItems;
	
	private List<String> colKeys;
	private List<String> sendKeys;
	private List<String> columns;
	
//	private List<OrderByElement> orderBy;
	//	List<List<String>> tableCol;
	boolean isFinalMerge;
	private Map<String, PrimitiveValue> map;
	
	
	public fileIterator(File file, List<PrimitiveValue> pm , List<String> colKeys , Map<String , PrimitiveValue> map)
	{
		this.csvFile= file.toString();
		this.pm = pm;	
		this.colKeys = colKeys;
//		this.map = map;
		try 
		{
			this.br = new BufferedReader(new FileReader(csvFile));

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		tuple = "";
		this.isFinalMerge = false;
	}
	
	public fileIterator(String fileName, List<SelectItem> selectItems , List<String> colKeys , List<PrimitiveValue> pm) 
	{
			this.csvFile = fileName;
			this.selectItems = selectItems;
			this.colKeys = colKeys;
			this.pm = pm;
			this.columns = new ArrayList<String>();
			this.sendKeys = new ArrayList<String>();
//			
			for(String col : colKeys)
			{
//				System.out.println( " col " + col);
				sendKeys.add(col);
			}
//			
			for(SelectItem sel : selectItems)
			{
//				System.out.println(" value " + sel);
				columns.add(sel.toString());
			}
			try {

				this.br = new BufferedReader(new FileReader(csvFile));
//				System.out.println(this.br.readLine());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			tuple = "";
			this.isFinalMerge  = true;
	}

	@Override
	public boolean hasNext() 
	{
		// TODO Auto-generated method stub
		try {
			if(br.ready()) {
				return true;
			}
			else return false;
		} catch (IOException e) {
			e.printStackTrace();
//			System.out.println("Error 2 " + tableName);
			return false;
			
		}
	} 

	@Override
	public Map<String, PrimitiveValue> next() {
		// TODO Auto-generated method stub
		if(this.hasNext())
		{
//			System.out.println( "in next"); 
			
			try {
				tuple = br.readLine();
//				System.out.println(tuple);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			Map<String,PrimitiveValue> map = this.map;
			
			map = new HashMap<String, PrimitiveValue>();
			
			String row[] = tuple.split("\\|");
//			System.out.println();
//			System.out.println( new ArrayList<String>(Arrays.asList(row)) + " col " + this.colKeys + " ppm " + pm);
//			System.out.println(" brefore " + row[0]);
			for (int i = 0; i < row.length; i++) {				
				String value = row[i];
				PrimitiveValue pmVal;
//					System.out.println( " " + pm.get(i).getType().toString());
				switch(pm.get(i).getType().toString().toLowerCase())
				 {
					case "int":
						pmVal = new LongValue(value);
						break;
					case "string":
						pmVal = new StringValue(value);
						break;
					case "long":
						pmVal = new LongValue(value);
						break;	
					case "double":
						pmVal = new DoubleValue(value);
						break;
					case "date":
						pmVal = new DateValue(value);
						break;
					default:
						pmVal = new StringValue(value);
						break;
					}
				if(isFinalMerge)
				{
					map.put(sendKeys.get(i) , pmVal);
				}
				else
				map.put(colKeys.get(i),pmVal);
			}
//			System.out.println(" create " + map);
			return map;
		}
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		try {
			br.close();
			br = new BufferedReader(new FileReader(this.csvFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
//			System.out.println("Error 1 " + tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<String> getColumns() {
		// TODO Auto-generated method stub
		return this.columns;
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return null;
	}
		
}
