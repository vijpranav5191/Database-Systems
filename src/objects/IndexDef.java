package objects;

import java.io.Serializable;

import net.sf.jsqlparser.statement.create.table.Index;

public class IndexDef  implements Serializable{
	private Index index;

	public Index getIndex() {
		return index;
	}

	public void setIndex(Index index) {
		this.index = index;
	}
}
