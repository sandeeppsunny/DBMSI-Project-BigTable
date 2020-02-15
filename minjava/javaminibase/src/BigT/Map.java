package BigT;

import global.GlobalConst;

public class Map implements GlobalConst{
	
	private String RowLabel;
	
	private String ColumnLabel;
	
	private int TimeStamp;
	
	private String Value;
	
	
	/* Maximum size of any tuple */
	public static final int max_size = MINIBASE_PAGESIZE;
	
	/* Byte array to store data */
	
	private byte [] data;
	
	/* Map will have 4 fixed fields */
	private static final int fldCnt = 4; 
	
	/* Start position of this map in data[] */
	private int map_offset;
	
	public Map() {
		this.data = new byte[max_size];
	    this.map_offset = 0;
	}
	
	public Map(Map  fromMap) {
		this.data = fromMap.getMapByteArray();
		this.map_offset = 0;
	}
	
	public Map(byte[] amap, int offset) {
		this.data = amap;
		this.map_offset = offset;
	}
	
	public String getRowLabel() {
		return RowLabel;
	}

	public void setRowLabel(String rowLabel) {
		RowLabel = rowLabel;
	}

	public String getColumnLabel() {
		return ColumnLabel;
	}

	public void setColumnLabel(String columnLabel) {
		ColumnLabel = columnLabel;
	}

	public int getTimeStamp() {
		return TimeStamp;
	}

	public void setTimeStamp(int timeStamp) {
		TimeStamp = timeStamp;
	}

	public String getValue() {
		return Value;
	}

	public void setValue(String value) {
		Value = value;
	}
	
	public byte[] getMapByteArray() {
		return null;
	}
	
	public void print() {
		
	}
	
	public int size() {
		return 0;
	}
	
	public Map mapCopy(Map fromMap) {
		return null;
	}
	
	public void mapInit(byte[] amap, int offset) {
		this.data = amap;
		this.map_offset = offset;
	}
	
	public void mapSet(byte[] frommap, int offset) {
		this.data = 
		
	}
	
}
