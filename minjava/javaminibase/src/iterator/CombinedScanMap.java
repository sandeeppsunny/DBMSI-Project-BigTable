package iterator;


import BigT.Map;
import BigT.Pair;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.MapIndexScan;


import java.lang.*;
import java.io.*;

/**
 * open a heapfile and according to the condition expression to get
 * output file, call get_next to get all maps
 */
public class CombinedScanMap extends MapIterator {
    private Map map1;
    private MapIterator[] iterators;


    /**
     * constructor
     *
     * @param file_name  heapfile to be opened
     * @param outFilter  select expressions
     * @throws IOException         some I/O fault
     * @throws FileScanException   exception from this class
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */
    public CombinedScanMap(String file_name,
                           CondExpr[] outFilter,
                           CondExpr[] index_2_filter,
                           CondExpr[] index_3_filter,
                           CondExpr[] index_4_filter,
                           CondExpr[] index_5_filter)
            throws IOException,
            FileScanException,
            TupleUtilsException,
            InvalidRelation {
        map1 = new Map();
        AttrType[] attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrString);
        short[] res_str_sizes = new short[]{Map.DEFAULT_STRING_ATTRIBUTE_SIZE,
                Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};

        try{
            iterators = new MapIterator[5];
            iterators[0] = new FileScanMap(file_name + "_" + 1, null, outFilter, false);
            // Only null exists in filter expression
            if(index_2_filter.length == 1) {
                iterators[1] = new FileScanMap(file_name + "_" + 2, null, outFilter, false);
            } else {
                iterators[1] = new MapIndexScan(new IndexType(IndexType.B_Index), file_name + "_" + 2, file_name + "_index_" + 2,
                                attrType, res_str_sizes, 4, 4, null, outFilter, index_2_filter, 1, false);
            }
            // Only null exists in filter expression
            if(index_3_filter.length == 1) {
                iterators[2] = new FileScanMap(file_name + "_" + 3, null, outFilter, false);
            } else {
                iterators[2] = new MapIndexScan(new IndexType(IndexType.B_Index), file_name + "_" + 3, file_name + "_index_" + 3,
                        attrType, res_str_sizes, 4, 4, null, outFilter, index_3_filter, 2, false);
            }
            // Only null exists in filter expression
            if(index_4_filter.length == 1) {
                iterators[3] = new FileScanMap(file_name + "_" + 4, null, outFilter, false);
            } else {
                iterators[3] = new MapIndexScan(new IndexType(IndexType.B_Index), file_name + "_" + 4, file_name + "_index_" + 4,
                        attrType, res_str_sizes, 4, 4, null, outFilter, index_4_filter, 2, false);
            }
            // Only null exists in filter expression
            if(index_5_filter.length == 1) {
                iterators[4] = new FileScanMap(file_name + "_" + 5, null, outFilter, false);
            } else {
                iterators[4] = new MapIndexScan(new IndexType(IndexType.B_Index), file_name + "_" + 5, file_name + "_index_" + 5,
                        attrType, res_str_sizes, 4, 4, null, outFilter, index_5_filter, 1, false);
            }
        } catch(Exception e) {
            throw new FileScanException(e, "CombinedScanMap creation failed");
        }
    }

    /**
     * @return the result map
     * @throws JoinsException                 some join exception
     * @throws IOException                    I/O errors
     * @throws InvalidTupleSizeException      invalid tuple size
     * @throws InvalidTypeException           tuple type not valid
     * @throws PageNotReadException           exception from lower layer
     * @throws PredEvalException              exception from PredEval class
     * @throws UnknowAttrType                 attribute type unknown
     * @throws FieldNumberOutOfBoundException array out of bounds
     * @throws WrongPermat                    exception for wrong FldSpec argument
     */
    public Map get_next()
            throws Exception {
        while (true) {
            if ((map1 = iterators[0].get_next()) == null) {
                break;
            }
            return map1;
        }

        while (true) {
            if ((map1 = iterators[1].get_next()) == null) {
                break;
            }
            return map1;
        }

        while (true) {
            if ((map1 = iterators[2].get_next()) == null) {
                break;
            }
            return map1;
        }

        while (true) {
            if ((map1 = iterators[3].get_next()) == null) {
                break;
            }
            return map1;
        }

        while (true) {
            if ((map1 = iterators[4].get_next()) == null) {
                return null;
            }
            return map1;
        }
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     */
    public void close() {
        try {
            for(int i=0; i<5; i++) {
                iterators[i].close();
            }
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred while closing combined scan!");
        }
    }

}