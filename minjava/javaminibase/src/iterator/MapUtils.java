package iterator;

import BigT.*;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;

import java.io.IOException;

/**
 * Useful methods for processing BigTable maps
 */
public class MapUtils {
    final static int MAP_TOTAL_FIELD_COUNT = 4;

    /**
     * This function compares a map with another map in respective field, and
     * returns:
     * <p>
     * 0        if the two are equal,
     * 1        if the first map is greater,
     * -1        if the first map is smaller,
     *
     * @param m1       one map.
     * @param m2       another map.
     * @param mapfldno the field numbers in the tuples to be compared.
     * @return 0        if the two are equal,
     * 1        if the tuple is greater,
     * -1        if the tuple is smaller,
     * @throws FieldNumberOutOfBoundException mapfldno provided is out of bounds
     * @throws IOException                    some I/O fault
     */
    public static int CompareMapWithMap(Map m1, Map m2, int mapfldno) throws FieldNumberOutOfBoundException, IOException {
        if ((mapfldno > 0) && (mapfldno <= MAP_TOTAL_FIELD_COUNT)) {
            if (mapfldno == 1) {
                return m1.getRowLabel().compareTo(m2.getRowLabel());
            } else if (mapfldno == 2) {
                return m1.getColumnLabel().compareTo(m2.getColumnLabel());
            } else if (mapfldno == 3) {
                if (m1.getTimeStamp() > m2.getTimeStamp()) {
                    return 1;
                } else if (m1.getTimeStamp() < m2.getTimeStamp()) {
                    return -1;
                } else {
                    return 0;
                }
            } else {
                return m1.getValue().compareTo(m2.getValue());
            }
        } else {
            throw new FieldNumberOutOfBoundException(null, "MAP:MAP_FLDNO_OUT_OF_BOUND");
        }
    }

    /**
     * This function Compares two maps in all fields
     *
     * @param m1 the first map
     * @param m2 the second map
     * @return 0        if the two are not equal,
     * 1        if the two are equal,
     * @throws IOException some I/O fault
     */
    public static boolean Equal(Map m1, Map m2) throws IOException {
        // Compare row, column, timestamp and value fields
        return m1.getRowLabel().equals(m2.getRowLabel()) && m1.getColumnLabel().equals(m2.getColumnLabel())
                && m1.getTimeStamp() == m2.getTimeStamp() && m1.getValue().equals(m2.getValue());
    }
}