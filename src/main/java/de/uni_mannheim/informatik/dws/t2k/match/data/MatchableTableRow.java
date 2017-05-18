package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import de.uni_mannheim.informatik.dws.winter.model.Fusible;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.utils.SparseArray;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * 
 * Model of a Web Table column.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRow implements Matchable, Fusible<MatchableTableColumn>, Serializable {

	private static final long serialVersionUID = 1L;
	
	public MatchableTableRow() {

	}

	public MatchableTableRow(String id) {
		this.id = id;
		this.tableId = -1;
		this.rowNumber = -1;
	}

	public MatchableTableRow(String id, Object[] values, int tableId, DataType[] types) {
		this.id = id;
		this.tableId = tableId;
		this.rowNumber = -1;

		this.indices = new int[values.length];
		SparseArray<Object> valuesSparse = new SparseArray<>(values);
		this.values = valuesSparse.getValues();
		for(int i=0; i<values.length;i++){
			indices[i] = i;
		}
		
		this.types = new DataType[values.length];
		for (int i = 0; i < indices.length; i++) {
			this.types[i] = types[i];
		}
	}
	
	public MatchableTableRow(TableRow row, int tableId) { 
		this.tableId = tableId;
		this.rowNumber = row.getRowNumber();
		this.id = row.getIdentifier();
		this.rowLength = row.getTable().getSchema().getSize();
		
		ArrayList<DataType> types = new ArrayList<>();
		ArrayList<TableColumn> cols = new ArrayList<>(row.getTable().getSchema().getRecords());
		Collections.sort(cols, new TableColumn.TableColumnByIndexComparator());
		for(TableColumn c : cols) {
			types.add(c.getDataType());
		}
		
		if(types.size()<row.getValueArray().length) {
			System.err.println("problem");
		}
		
		SparseArray<Object> valuesSparse = new SparseArray<>(row.getValueArray());
		this.values = valuesSparse.getValues();
		this.indices = valuesSparse.getIndices();
		
		this.types = new DataType[values.length];
		for (int i = 0; i < indices.length; i++) {
			this.types[i] = types.get(indices[i]);
		}
	}
	
	protected String id;
	private DataType[] types;
	private Object[] values;
	private int[] indices;
	private int rowNumber;
	private int tableId;
	private int rowLength; // total number of columns (including null values)
	
	@Override
	public String getIdentifier() {
		return id;
	}

	@Override
	public String getProvenance() {
		return null;
	}

	public int getNumCells() {
		return values.length;
	}
	public Object get(int columnIndex) {
		if(indices!=null) {
			return SparseArray.get(columnIndex, values, indices);
		} else {
			return values[columnIndex];
		}
	}
	
	/**
	 * Sets the respective value. If the value didn't exist before, the sparse representation is replaced by a dense representation, which can lead to higher memory consumption, but is faster when setting multiple values 
	 * @param columnIndex
	 * @param value
	 */
	public void set(int columnIndex, Object value) {
		int maxLen = columnIndex+1;
		
		if(indices!=null) {
			maxLen = Math.max(maxLen, indices[indices.length-1]+1);
			
			Object[] allValues = new Object[maxLen];
			for(int i=0;i<indices.length;i++) {
				allValues[indices[i]] = values[i];
			}
			
			values = allValues;
			indices = null;
		} else {
			if(maxLen>values.length) {
				values = Arrays.copyOf(values, maxLen);
			}
		}
		
		values[columnIndex] = value;
	}
	
	public DataType getType(int columnIndex) {
		if(indices!=null) {
			int idx = SparseArray.translateIndex(columnIndex, indices);
			
			if(idx==-1) {
				return null;
			} else {
				return types[idx];
			}
		} else {
			return types[columnIndex];
		}
	}
	public Object[] getValues() {
		return values;
	}
	public DataType[] getTypes() {
		return types;
	}
	public int getRowNumber() {
		return rowNumber;
	}
	public int getTableId() {
		return tableId;
	}
	
	/**
	 * @return the rowLength
	 */
	public int getRowLength() {
		return rowLength;
	}
	
	public boolean hasColumn(int columnIndex) {
		if(indices!=null) {
			int idx = SparseArray.translateIndex(columnIndex, indices);
		
			return idx!=-1;
		} else {
			return columnIndex < values.length;
		}
	}


	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Fusable#hasValue(java.lang.String)
	 */
	@Override
	public boolean hasValue(MatchableTableColumn attribute) {
		return hasColumn(attribute.getColumnIndex()) && get(attribute.getColumnIndex())!=null;		
	}
	
	public String format(int columnWidth) {
		StringBuilder sb = new StringBuilder();
		
		boolean first=true;
		for(int i = 0; i < rowLength; i++) {
			
			if(!first) {
				sb.append(" | ");
			}
			
			String value;
			if(hasColumn(i)) {
				Object v = get(i);
				value = v.toString();
			} else {
				value = "null";
			}
			
			sb.append(padRight(value,columnWidth));

			first = false;
		}
		
		return sb.toString();
	}
	
    protected String padRight(String s, int n) {
        if(n==0) {
            return "";
        }
        if (s.length() > n) {
            s = s.substring(0, n);
        }
        s = s.replace("\n", " ");
        return String.format("%1$-" + n + "s", s);
    }
}
