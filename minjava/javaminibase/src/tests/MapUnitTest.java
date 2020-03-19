package tests;

import BigT.Map;
import global.MID;
import heap.Heapfile;
import iterator.MapUtils;

public class MapUnitTest {
    final static String ROW_LABEL = "RowLabel";
    final static String COL_LABEL = "ColumnLabel";
    final static int TIME_STAMP = 25;
    final static String VALUE = "Value";
    final static short[] strSizes = new short[]{(short)ROW_LABEL.length(), (short)COL_LABEL.length(), (short)VALUE.length()};
//    final static int MAP_SIZE = 82;

    /*
        Tests basic functionality of map class
     */
    public static void test1() {
        try {
            Map m = new Map();
            m.setHdr((short)4, null, strSizes);
            m.setRowLabel(ROW_LABEL);
            m.setColumnLabel(COL_LABEL);
            m.setTimeStamp(TIME_STAMP);
            m.setValue(VALUE);

            if (!m.getRowLabel().equals(ROW_LABEL)) {
                throw new Exception("Unexpected row value returned!");
            }
            if (!m.getColumnLabel().equals(COL_LABEL)) {
                throw new Exception("Unexpected column value returned!");
            }
            if (m.getTimeStamp() != TIME_STAMP) {
                throw new Exception("Unexpected timestamp value returned!");
            }
            if (!m.getValue().equals(VALUE)) {
                throw new Exception("Unexpected value returned!");
            }
//            if (m.size() != MAP_SIZE) {
//                throw new Exception("Unexpected map size returned!");
//            }
            System.out.println("Test-1 passed as expected!");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /*
        Tests basic functionality of map utils class
    */
    public static void test2() {
        try {
            Map m1 = new Map();
            m1.setHdr((short)4, null, strSizes);
            m1.setRowLabel(ROW_LABEL);
            m1.setColumnLabel(COL_LABEL);
            m1.setTimeStamp(TIME_STAMP);
            m1.setValue(VALUE);

            Map m2 = new Map();
            m2.setHdr((short)4, null, strSizes);
            m2.setRowLabel(ROW_LABEL);
            m2.setColumnLabel(COL_LABEL);
            m2.setTimeStamp(TIME_STAMP);
            m2.setValue(VALUE);

            if (!MapUtils.Equal(m1, m2)) {
                throw new Exception("Maps returned not equal! Unexpected failure!");
            }

            if (!(MapUtils.CompareMapWithMap(m1, m2, 1) == 0 && MapUtils.CompareMapWithMap(m1, m2, 2) == 0
                    && MapUtils.CompareMapWithMap(m1, m2, 3) == 0 && MapUtils.CompareMapWithMap(m1, m2, 4) == 0)) {
                throw new Exception("Maps compared with field but returned unexpected result!");
            }
            System.out.println("Test-2 passed as expected!");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void main(String argv[]) {
        test1();
        test2();
    }
}