package dubstep;

import java.util.List;

import javax.xml.validation.Schema;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

class Evaluate extends Eval{
 
		public PrimitiveValue eval(Column col){
			if(col.getTable() != null && col.getTable().getName() != null){
				List<ColumnDefs> cd = SchemaStructure.schema.get(col.getTable().getName());
			}
			return null;
		}
}