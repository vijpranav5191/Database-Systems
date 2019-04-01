package iterators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

//import javafx.scene.control.TableColumn;

import java.util.ArrayList;
//import java.util.Arrays;
import java.util.HashMap;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
//import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;
//import objects.ColumnDefs;
//import objects.SchemaStructure;

public class fileIterator implements DefaultIterator {
	
	private String csvFile;
	List<PrimitiveValue> pm;
//	private String tableName;
	private BufferedReader br;
	private String tuple;
//	private Table tab;
	private Map<String, PrimitiveValue> map;
	private List<SelectItem> selectItems;
	private List<String> colKeys;
	private List<String> sendKeys;
//	private List<OrderByElement> orderBy;
	//	List<List<String>> tableCol;
	boolean isFinalMerge;
	
	
	public fileIterator(File file, List<PrimitiveValue> pm , List<String> colKeys)
	{
		this.csvFile= file.toString();
		this.pm = pm;	
		this.colKeys = colKeys;
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
		
//		System.out.println(" here " + selectItems + " pmvalue " + pm + " colKeys " + colKeys );
			this.csvFile = fileName;
			this.selectItems = selectItems;
			this.colKeys = colKeys;
			this.pm = pm;
			sendKeys = new ArrayList<String>();
			for(String  col : colKeys)
			{
				
				String x = (col.split("\\."))[1]; 
				for(SelectItem s : selectItems)
				{
					if((s.toString().split("\\.")).length == 2 && !sendKeys.contains(s.toString()))
					{
						sendKeys.add(s.toString());
					}
					else
					{
						if(x.equals(s.toString()) && !sendKeys.contains(s.toString()))
						{
							sendKeys.add(col);
						}
					}
				}
				
				
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
//			this.tableCol = new ArrayList<List<String>>();
//			for(int sVar=0; sVar< selectItems.size(); sVar++)
//			{
//				tableCol.add(sVar , new ArrayList<String>(Arrays.asList( selectItems.get(sVar).split("\\.")[0] , selectItems.get(sVar).split("\\.")[1] )) );
//			}
		// TODO Auto-generated constructor stub
			tuple = "";
			
//			System.out.println( " send Keys "  + sendKeys);
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
			map = new HashMap<String , PrimitiveValue>();
			
			String row[] = tuple.split("\\|");
//			System.out.println(" brefore " + row[0]);
			for (int i = 0; i < row.length; i++) {				
				String value = row[i];
				PrimitiveValue pmVal;
//					System.out.println( " " + pm.get(i).getType().toString());
				switch(pm.get(i).getType().toString())
				 {
					case "INTEGER":
						pmVal = new LongValue(value);
						break;
					case "STRING":
						pmVal = new StringValue(value);
						break;
					case "LONG":
						pmVal = new LongValue(value);
						break;	
					case "DOUBLE":
						pmVal = new DoubleValue(value);
						break;
					case "DATE":
						pmVal = new DateValue(value);
						break;
					default:
						pmVal = new StringValue(value);
						break;
					}
				if(isFinalMerge)
					map.put(sendKeys.get(i) , pmVal);
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
		return this.sendKeys;
	}

	@Override
	public DefaultIterator getChildIter() {
		// TODO Auto-generated method stub
		return null;
	}
		
}
