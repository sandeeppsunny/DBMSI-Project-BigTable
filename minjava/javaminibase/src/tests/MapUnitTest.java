package tests;

import BigT.Map;

public class MapUnitTest {
    final static String ROW_LABEL = "RowLabel-test";
    final static String COL_LABEL = "ColumnLabel-test";
    final static int TIME_STAMP = 2;
    final static String VALUE = "Value-test";
    final static int MAP_SIZE = 78;

    /*
        Tests basic functionality of map class
     */
    public static void test1() {
        try {
            Map m = new Map();
            m.setRowLabel(ROW_LABEL);
            m.setColumnLabel(COL_LABEL);
            m.setTimeStamp(TIME_STAMP);
            m.setValue(VALUE);

            if(!m.getRowLabel().equals(ROW_LABEL)) {
                throw new Exception("Unexpected row value returned!");
            }
            if(!m.getColumnLabel().equals(COL_LABEL)) {
                throw new Exception("Unexpected column value returned!");
            }
            if(m.getTimeStamp() != TIME_STAMP) {
                throw new Exception("Unexpected timestamp value returned!");
            }
            if(!m.getValue().equals(VALUE)) {
                throw new Exception("Unexpected value returned!");
            }
            if(m.size() != MAP_SIZE) {
                throw new Exception("Unexpected value returned!");
            }
            System.out.println("Test passed as expected!");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void main(String argv[]) {
        test1();
    }
}