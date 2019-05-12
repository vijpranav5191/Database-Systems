package utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import iterators.FileReaderIterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class ColumnSeparator {
	Table table;
	FileReaderIterator iter;
	List<String> columns;
	List<BufferedWriter> writers;
	
	public ColumnSeparator(Table table, List<String> columns, String pathToDir){
		this.table = table;
		this.columns = columns;
		this.writers = new ArrayList<>();
		iter = new FileReaderIterator(this.table);
		for(int i = 0; i < this.columns.size(); i++) {
			String cols = columns.get(i);
			File filename = new File(pathToDir + this.table.getName() + "." + cols);
			BufferedWriter writer;
			try {
				writer = new BufferedWriter(new FileWriter(filename, true));
				writers.add(writer);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void execute() throws IOException {
		while(this.iter.hasNext()) {
			String next = this.iter.next();
			String[] arr = next.split("\\|");
			for(int i = 0; i < columns.size();i++) {
				BufferedWriter writer = this.writers.get(i);
				writer.write(arr[i]);
				writer.newLine();
			}
		}
	}
	
	public void executeSingle(List<Expression> exps) throws IOException {
		for(int i = 0; i < columns.size();i++) {
			BufferedWriter writer = this.writers.get(i);
			writer.write(exps.get(i).toString());
			writer.newLine();
		}
	}
	
	public void close() {
		for(BufferedWriter bwr: this.writers) {
			try {
				bwr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
