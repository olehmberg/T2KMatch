package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.io.Serializable;
import java.util.Map;

import de.uni_mannheim.informatik.dws.winter.model.Fusible;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumnStatistics;

/**
 * 
 * Model of a Web Table column.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableColumn implements Matchable, Fusible<MatchableTableColumn>, Comparable<MatchableTableColumn>, Serializable {

	private static final long serialVersionUID = 1L;
	protected int tableId;
	protected int columnIndex;
	protected String id;
	protected DataType type;
	protected String header;
	protected TableColumnStatistics statistics;
	protected Object min, max;
	
	public int getTableId() {
		return tableId;
	}
	public int getColumnIndex() {
		return columnIndex;
	}
	protected void setColumnIndex(int index) {
		columnIndex = index;
	}
	
	/**
	 * @return the header
	 */
	public String getHeader() {
		return header;
	}
	/**
	 * @param header the header to set
	 */
	public void setHeader(String header) {
		this.header = header;
	}
	
	/**
	 * @return TableColumnStatistic the statistics
	 */
	public TableColumnStatistics getStatistics() {
		return statistics;
	}
	
	/**
	 * @param statistics the statistics to set
	 */
	public void setStatistics(TableColumnStatistics statistics) {
		this.statistics = statistics;
	}



	public static final int CSV_LENGTH = 4;
	
	public static MatchableTableColumn fromCSV(String[] values, Map<String, Integer> tableIndices) {
		MatchableTableColumn c = new MatchableTableColumn();
		c.tableId = tableIndices.get(values[0]);
		c.columnIndex = Integer.parseInt(values[1]);
		c.type = DataType.valueOf(values[2]);
		c.id = values[3];
		return c;
	}
	
	public MatchableTableColumn() {
		
	}
	
	public MatchableTableColumn(int tableId, int columnIndex, String header, DataType type){
		this.tableId = tableId;
		this.columnIndex = columnIndex;
		this.id = "";
		this.header = header;
		this.type = type;
	}
	
	public MatchableTableColumn(int tableId, TableColumn c) {
		this.tableId = tableId;
		this.columnIndex = c.getColumnIndex();
		this.type = c.getDataType();
		this.header = c.getHeader();
		
		// this controls the schema that we are matching to!
		// using c.getIdentifier() all dbp properties only exist once! (i.e. we cannot handle "_label" columns and the value of tableId is more or less random
		this.id = c.getUniqueName();
	}
	
	@Override
	public String getIdentifier() {
		return id;
	}

	@Override
	public String getProvenance() {
		return null;
	}

	/**
	 * @return the type
	 */
	public DataType getType() {
		return type;
	}
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Fusable#hasValue(java.lang.Object)
	 */
	@Override
	public boolean hasValue(MatchableTableColumn attribute) {
		return false;
	}
	
	/**
	 * @return the min
	 */
	public Object getMin() {
		return min;
	}
	/**
	 * @return the max
	 */
	public Object getMax() {
		return max;
	}
	/**
	 * @param min the min to set
	 */
	public void setMin(Object min) {
		this.min = min;
	}
	/**
	 * @param max the max to set
	 */
	public void setMax(Object max) {
		this.max = max;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(MatchableTableColumn o) {
		return getIdentifier().compareTo(o.getIdentifier());
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return getIdentifier().hashCode();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof MatchableTableColumn) {
			MatchableTableColumn col = (MatchableTableColumn)obj;
			return getIdentifier().equals(col.getIdentifier());
		} else {
			return super.equals(obj);
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("{#%d}[%d]%s", getTableId(), getColumnIndex(), getHeader());
	}
	
}
