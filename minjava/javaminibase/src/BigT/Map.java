package BigT;


public class Map {
	
	private String RowLabel;
	
	private String ColumnLabel;
	
	private int TimeStamp;
	
	private String Value;
	
	public Map() {
		
	}
	
	public Map(Map  fromMap) {
		
	}
	
	public Map(byte[] amap, int offset) {
		
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
	
	public Map mapInit(byte[] amap, int offset) {
		return null;
	}
	
	public void mapSet(byte[] frommap, int offset) {
		
	}
	
}
