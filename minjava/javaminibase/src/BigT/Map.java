package BigT;

import java.io.IOException;

import global.AttrType;
import global.Convert;
import global.GlobalConst;

/**
 * Map object analogous to tuple
 * Fields: rowLabel, columnLabel, timeStamp, Value.
 * Structure of Map:
 * (RowLabel String (20 bytes + 2 bytes); ColumnLabel String (20 bytes + 2bytes); TimeStamp Integer (4 bytes); Value String (20 bytes + 2 bytes) )
 */
public class Map implements GlobalConst {

    /* Maximum size of any tuple */
    public static final int max_size = MINIBASE_PAGESIZE;

    /* Byte array to store data */

    private byte[] data;

    /* Map will have 4 fixed fields */
    private static final int fldCnt = 4;

    /* Start position of this map in data[] */
    private int map_offset;

    /* Array of offsets of the fields */

    private short[] fldOffset;

    private static final int stringAttributeSize = 20;

    private static final int integerAttributeSize = 4;

    public static final int MAP_LENGTH = 78;

    /**
     * Default Map constructor
     */
    public Map() throws IOException {
        this.data = new byte[MAP_LENGTH];
        this.map_offset = 0;
        fldOffset = new short[fldCnt + 2];

        fldOffset[0] = 8;
        fldOffset[1] = (short) (fldOffset[0] + stringAttributeSize + 2);
        fldOffset[2] = (short) (fldOffset[1] + stringAttributeSize + 2);
        fldOffset[3] = (short) (fldOffset[2] + integerAttributeSize);
        fldOffset[4] = (short) (fldOffset[3] + stringAttributeSize + 2);
        fldOffset[5] = (short) MAP_LENGTH;

        for (int i = 0; i <= fldCnt + 1; i++) {
            Convert.setShortValue(fldOffset[i], 2 * i, data);
        }
    }

    /**
     * Map constructor to create a map from another map
     *
     * @param fromMap
     */
    public Map(Map fromMap) {
        this.data = fromMap.getMapByteArray();
        this.map_offset = 0;
        fldOffset = new short[fldCnt + 2];

        fldOffset[0] = 8;
        fldOffset[1] = (short) (fldOffset[0] + stringAttributeSize + 2);
        fldOffset[2] = (short) (fldOffset[1] + stringAttributeSize + 2);
        fldOffset[3] = (short) (fldOffset[2] + integerAttributeSize);
        fldOffset[4] = (short) (fldOffset[3] + stringAttributeSize + 2);
        fldOffset[5] = (short) MAP_LENGTH;
    }

    /**
     * Map constructor to create a map object from a byte array and a given offset.
     *
     * @param amap
     * @param offset
     */
    public Map(byte[] amap, int offset) {
        this.data = amap;
        this.map_offset = offset;
        fldOffset = new short[fldCnt + 2];
        fldOffset[0] = 8;
        fldOffset[1] = (short) (fldOffset[0] + stringAttributeSize + 2);
        fldOffset[2] = (short) (fldOffset[1] + stringAttributeSize + 2);
        fldOffset[3] = (short) (fldOffset[2] + integerAttributeSize);
        fldOffset[4] = (short) (fldOffset[3] + stringAttributeSize + 2);
        fldOffset[5] = (short) MAP_LENGTH;
    }

//	public void setHdr(short numFlds, AttrType types[], short strSizes[]) throws IOException  {
//		
//	}

    /**
     * Get the rowlabel field (String).
     * Length of string calculated by taking difference between the offset of current filed and next field.
     *
     * @return rowLabel
     * @throws IOException
     */
    public String getRowLabel() throws IOException {
        String rowLabel;
        rowLabel = Convert.getStrValue(fldOffset[0], this.data, fldOffset[1] - fldOffset[0]);
        return rowLabel;
    }

    /**
     * Set the rowlabel field of the map
     *
     * @param rowLabel
     * @return Map object
     * @throws IOException
     */
    public Map setRowLabel(String rowLabel) throws IOException {
        Convert.setStrValue(rowLabel, fldOffset[0], this.data);
        return this;
    }

    /**
     * Get the columnLabel field
     *
     * @return columnLabel after successful Conversion.
     * Length of string calculated by taking difference between the offset of current filed and next field.
     * @throws IOException
     */
    public String getColumnLabel() throws IOException {
        String columnLabel;
        columnLabel = Convert.getStrValue(fldOffset[1], this.data, fldOffset[2] - fldOffset[1]);
        return columnLabel;
    }

    /**
     * Set the field ColumnLabel
     *
     * @param columnLabel
     * @return Map object
     * @throws IOException
     */
    public Map setColumnLabel(String columnLabel) throws IOException {
        Convert.setStrValue(columnLabel, fldOffset[1], this.data);
        return this;
    }

    /**
     * Get the timestamp value given the data bytearray
     * and position of timestamp field is 3rd in the fieldOffset.
     *
     * @return timestamp value: Integer value if conversion is done
     * @throws IOException I/O errors
     */
    public int getTimeStamp() throws IOException {
        int val;
        val = Convert.getIntValue(fldOffset[2], this.data);

        return val;
    }

    /**
     * Method used to set the timeStamp field.
     *
     * @param timeStamp
     * @return Map object after setting the TimeStamp field
     * @throws IOException I/O Errors in Convert.
     */
    public Map setTimeStamp(int timeStamp) throws IOException {
        Convert.setIntValue(timeStamp, fldOffset[2], this.data);
        return this;
    }

    /**
     * Get the value field (String field).
     * Length of string calculated by taking difference between the offset of current filed and next field.
     *
     * @return value
     * @throws IOException
     */
    public String getValue() throws IOException {
        String value;
        value = Convert.getStrValue(fldOffset[3], this.data, fldOffset[4] - fldOffset[3]);
        return value;
    }

    /**
     * Set the Value field of the Map
     *
     * @param value
     * @return Map object
     * @throws IOException I/O Errors
     */
    public Map setValue(String value) throws IOException {
        Convert.setStrValue(value, fldOffset[3], this.data);
        return this;
    }

    /**
     * @return map byte array
     */
    public byte[] getMapByteArray() {
        byte[] mapCopy = new byte[MAP_LENGTH];
        System.arraycopy(data, map_offset, mapCopy, 0, MAP_LENGTH);
        return mapCopy;
    }

    public void print() throws IOException {
        String rowLabel = getRowLabel();
        String columnLabel = getColumnLabel();
        int timeStamp = getTimeStamp();
        String value = getValue();
        System.out.println("[" + rowLabel + " " + columnLabel + " " + timeStamp + " ] -> " + value);
    }

    /**
     * Calculates size of map.
     *
     * @return size of the map
     */
    public short size() {
        return ((short) (fldOffset[fldCnt] - map_offset));
    }

    /**
     * Copy a map from another
     *
     * @param fromMap
     * @return Map
     */
    public Map mapCopy(Map fromMap) {
        return null;
    }

    /**
     * Method used when not using the constructor
     *
     * @param amap
     * @param offset
     */
    public void mapInit(byte[] amap, int offset) {
        this.data = amap;
        this.map_offset = offset;
    }

    /**
     * set a map with given byte array and offset.
     *
     * @param frommap
     * @param offset
     */
    public void mapSet(byte[] frommap, int offset) {
        System.arraycopy(frommap, offset, this.data, 0, frommap.length);
        this.map_offset = 0;
    }

    /**
     * get the offset of a tuple
     *
     * @return offset of the tuple in byte array
     */
    public int getOffset() {
        return map_offset;
    }
}
