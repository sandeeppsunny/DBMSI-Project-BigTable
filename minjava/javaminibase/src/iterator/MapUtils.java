package iterator;

import BigT.*;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;

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
     * 1        if the map is greater,
     * -1        if the map is smaller,
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
            } else{
                int m1_value_length = m1.getValue().length();
                int m2_value_length = m2.getValue().length();
                if(m1_value_length > m2_value_length){
                    return 1;
                }else if(m1_value_length < m2_value_length){
                    return -1;
                }else{
                    return m1.getValue().compareTo(m2.getValue());
                }
            }
        } else {
            throw new FieldNumberOutOfBoundException(null, "MAP:MAP_FLDNO_OUT_OF_BOUND");
        }
    }

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
     * @return 0        if the two are equal,
     * 1        if the map is greater,
     * -1        if the map is smaller,
     * @throws IOException                    some I/O fault
     */
    public static int CompareMapWithMapFirstType(Map m1, Map m2) throws IOException {
        int rowComp =  m1.getRowLabel().compareTo(m2.getRowLabel());
        if(rowComp == 0) {
            int colComp = m1.getColumnLabel().compareTo(m2.getColumnLabel());
            if(colComp == 0) {
                return CompareMapWithMapFifthType(m1, m2);
            } else {
                return colComp;
            }
        } else {
            return rowComp;
        }
    }

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
     * @return 0        if the two are equal,
     * 1        if the map is greater,
     * -1        if the map is smaller,
     * @throws IOException                    some I/O fault
     */
    public static int CompareMapWithMapSecondType(Map m1, Map m2) throws IOException {
        int colComp = m1.getColumnLabel().compareTo(m2.getColumnLabel());
        if(colComp == 0) {
            int rowComp = m1.getRowLabel().compareTo(m2.getRowLabel());
            if(rowComp == 0) {
                return CompareMapWithMapFifthType(m1, m2);
            } else {
                return rowComp;
            }
        } else {
            return colComp;
        }
    }

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
     * @return 0        if the two are equal,
     * 1        if the map is greater,
     * -1        if the map is smaller,
     * @throws IOException                    some I/O fault
     */
    public static int CompareMapWithMapThirdType(Map m1, Map m2) throws IOException {
        int rowComp = m1.getRowLabel().compareTo(m2.getRowLabel());
        if(rowComp == 0) {
            return CompareMapWithMapFifthType(m1, m2);
        } else {
            return rowComp;
        }
    }

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
     * @return 0        if the two are equal,
     * 1        if the map is greater,
     * -1        if the map is smaller,
     * @throws IOException                    some I/O fault
     */
    public static int CompareMapWithMapFourthType(Map m1, Map m2) throws IOException {
        int colComp = m1.getColumnLabel().compareTo(m2.getColumnLabel());
        if(colComp == 0) {
            return CompareMapWithMapFifthType(m1, m2);
        } else {
            return colComp;
        }
    }

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
     * @return 0        if the two are equal,
     * 1        if the map is greater,
     * -1        if the map is smaller,
     * @throws IOException                    some I/O fault
     */
    public static int CompareMapWithMapFifthType(Map m1, Map m2) throws IOException {
        if (m1.getTimeStamp() > m2.getTimeStamp()) {
            return 1;
        } else if (m1.getTimeStamp() < m2.getTimeStamp()) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int CompareMapWithMapSixthType(Map m1, Map m2) throws IOException{
        int rowComp = m1.getRowLabel().compareTo(m2.getRowLabel());
        if(rowComp == 0) {
            return m1.getValue().compareTo(m2.getValue());
        } else {
            return rowComp;
        }
    }

    public static int CompareMapWithMapValues(Map m1, Map m2) throws IOException{
        return m1.getValue().compareTo(m2.getValue());
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
    /**
     * set up the Jtuple's attrtype, string size,field number for using join
     *
     * @param Jtuple       reference to an actual tuple  - no memory has been malloced
     * @param res_attrs    attributes type of result tuple
     * @param in1          array of the attributes of the tuple (ok)
     * @param len_in1      num of attributes of in1
     * @param in2          array of the attributes of the tuple (ok)
     * @param len_in2      num of attributes of in2
     * @param t1_str_sizes shows the length of the string fields in S
     * @param t2_str_sizes shows the length of the string fields in R
     * @param proj_list    shows what input fields go where in the output tuple
     * @param nOutFlds     number of outer relation fileds
     * @throws IOException         some I/O fault
     * @throws TupleUtilsException exception from this class
     */
    public static short[] setup_op_map(Map Jmap, AttrType[] res_attrs,
                                         AttrType in1[], int len_in1, AttrType in2[],
                                         int len_in2, short t1_str_sizes[],
                                         short t2_str_sizes[],
                                         FldSpec proj_list[], int nOutFlds)
            throws IOException,
            TupleUtilsException {
//        short[] sizesT1 = new short[len_in1];
//        short[] sizesT2 = new short[len_in2];
        int i, count = 0;

//        for (i = 0; i < len_in1; i++)
//            if (in1[i].attrType == AttrType.attrString)
//                sizesT1[i] = t1_str_sizes[count++];

//        for (count = 0, i = 0; i < len_in2; i++)
//            if (in2[i].attrType == AttrType.attrString)
//                sizesT2[i] = t2_str_sizes[count++];

        int n_strs = Map.noOfStrFields;
//        for (i = 0; i < nOutFlds; i++) {
//            if (proj_list[i].relation.key == RelSpec.outer)
//                res_attrs[i] = new AttrType(in1[proj_list[i].offset - 1].attrType);
//            else if (proj_list[i].relation.key == RelSpec.innerRel)
//                res_attrs[i] = new AttrType(in2[proj_list[i].offset - 1].attrType);
//        }

        // Now construct the res_str_sizes array.
//        for (i = 0; i < nOutFlds; i++) {
//            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
//                n_strs++;
//            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset - 1].attrType == AttrType.attrString)
//                n_strs++;
//        }

        short[] res_str_sizes = new short[]{Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};
        count = 0;
//        for (i = 0; i < nOutFlds; i++) {
//            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
//                res_str_sizes[count++] = sizesT1[proj_list[i].offset - 1];
//            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset - 1].attrType == AttrType.attrString)
//                res_str_sizes[count++] = sizesT2[proj_list[i].offset - 1];
//        }
        try {
            Jmap.setHdr((short) nOutFlds, null, res_str_sizes);
        } catch (Exception e) {
            throw new TupleUtilsException(e, "setHdr() failed");
        }
        return res_str_sizes;
    }


    /**
     * set up the Jtuple's attrtype, string size,field number for using project
     *
     * @param Jtuple       reference to an actual tuple  - no memory has been malloced
     * @param res_attrs    attributes type of result tuple
     * @param in1          array of the attributes of the tuple (ok)
     * @param len_in1      num of attributes of in1
     * @param t1_str_sizes shows the length of the string fields in S
     * @param proj_list    shows what input fields go where in the output tuple
     * @param nOutFlds     number of outer relation fileds
     * @throws IOException         some I/O fault
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */

    public static short[] setup_op_map(Map Jmap, AttrType res_attrs[],
                                         AttrType in1[], int len_in1,
                                         short t1_str_sizes[],
                                         FldSpec proj_list[], int nOutFlds)
            throws IOException,
            TupleUtilsException,
            InvalidRelation {
//        short[] sizesT1 = new short[len_in1];
        int i, count = 0;

//        for (i = 0; i < len_in1; i++)
//            if (in1[i].attrType == AttrType.attrString)
//                sizesT1[i] = t1_str_sizes[count++];

        int n_strs = Map.DEFAULT_STRING_ATTRIBUTE_SIZE;
//        for (i = 0; i < nOutFlds; i++) {
//            if (proj_list[i].relation.key == RelSpec.outer)
//                res_attrs[i] = new AttrType(in1[proj_list[i].offset - 1].attrType);
//
//            else throw new InvalidRelation("Invalid relation -innerRel");
//        }

        // Now construct the res_str_sizes array.
//        for (i = 0; i < nOutFlds; i++) {
//            if (proj_list[i].relation.key == RelSpec.outer
//                    && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
//                n_strs++;
//        }

        short[] res_str_sizes = new short[]{Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};
        count = 0;
//        for (i = 0; i < nOutFlds; i++) {
//            if (proj_list[i].relation.key == RelSpec.outer
//                    && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
//                res_str_sizes[count++] = sizesT1[proj_list[i].offset - 1];
//        }

        try {
            Jmap.setHdr((short) nOutFlds, null, res_str_sizes);
        } catch (Exception e) {
            throw new TupleUtilsException(e, "setHdr() failed");
        }
        return res_str_sizes;
    }

}
