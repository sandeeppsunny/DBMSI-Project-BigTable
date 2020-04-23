package iterator;


import BigT.Map;
import BigT.Pair;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;


import java.lang.*;
import java.io.*;

/**
 * open a heapfile and according to the condition expression to get
 * output file, call get_next to get all maps
 */
public class FileScanMap extends MapIterator {
    private Heapfile f;
    private Scan scan;
    private Map map1;
    private Map Jmap;
    private CondExpr[] OutputFilter;
    public FldSpec[] perm_mat;
    private Heapfile[] heapfiles;
    private Scan[] scans;
    private boolean combined;


    /**
     * constructor
     *
     * @param file_name  heapfile to be opened
     * @param proj_list  shows what input fields go where in the output map
     * @param outFilter  select expressions
     * @throws IOException         some I/O fault
     * @throws FileScanException   exception from this class
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */
    public FileScanMap(String file_name,
                       FldSpec[] proj_list,
                       CondExpr[] outFilter, boolean combined
    )
            throws IOException,
            FileScanException,
            TupleUtilsException,
            InvalidRelation {

        OutputFilter = outFilter;
        perm_mat = proj_list;
        map1 = new Map();
        this.combined = combined;

        if(this.combined){
            try{
                heapfiles = new Heapfile[5];
                heapfiles[0] = new Heapfile(file_name + "_" + 1);
                heapfiles[1] = new Heapfile(file_name + "_" + 2);
                heapfiles[2] = new Heapfile(file_name + "_" + 3);
                heapfiles[3] = new Heapfile(file_name + "_" + 4);
                heapfiles[4] = new Heapfile(file_name + "_" + 5);
            }catch(Exception e){
                throw new FileScanException(e, "FileScanMap.java: Create heapfile failed");
            }

            try{
                scans = new Scan[5];
                scans[0] = heapfiles[0].openScanMap();
                scans[1] = heapfiles[1].openScanMap();
                scans[2] = heapfiles[2].openScanMap();
                scans[3] = heapfiles[3].openScanMap();
                scans[4] = heapfiles[4].openScanMap();
            }catch(Exception e){
                throw new FileScanException(e, "FileScanMap.java: openScan() failed");
            }
        }else{
            try {
                f = new Heapfile(file_name);

            } catch (Exception e) {
                throw new FileScanException(e, "Create new heapfile failed");
            }

            try {
                scan = f.openScanMap();
            } catch (Exception e) {
                throw new FileScanException(e, "openScan() failed");
            }
        }

    }

    /**
     * @return shows what input fields go where in the output map
     */
    public FldSpec[] show() {
        return perm_mat;
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
            throws JoinsException,
            IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            UnknowAttrType,
            FieldNumberOutOfBoundException,
            WrongPermat {
        MID mid = new MID();

        if(this.combined){
            while (true) {
                if ((map1 = scans[0].getNextMap(mid)) == null) {
                    break;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return map1;
                }
            }

            while (true) {
                if ((map1 = scans[1].getNextMap(mid)) == null) {
                    break;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return map1;
                }
            }

            while (true) {
                if ((map1 = scans[2].getNextMap(mid)) == null) {
                    break;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return map1;
                }
            }

            while (true) {
                if ((map1 = scans[3].getNextMap(mid)) == null) {
                    break;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return map1;
                }
            }

            while (true) {
                if ((map1 = scans[4].getNextMap(mid)) == null) {
                    return null;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return map1;
                }
            }
        }
        else{
            while (true) {
                if ((map1 = scan.getNextMap(mid)) == null) {
                    return null;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return map1;
                }
            }
        }
    }

    public Pair get_next_mid()
            throws JoinsException,
            IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            UnknowAttrType,
            FieldNumberOutOfBoundException,
            WrongPermat {
        MID mid = new MID();

        if(this.combined){
            while (true) {
                if ((map1 = scans[0].getNextMap(mid)) == null) {
                    break;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return new Pair(map1, mid);
                }
            }
            while (true) {
                if ((map1 = scans[1].getNextMap(mid)) == null) {
                    break;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return new Pair(map1, mid);
                }
            }
            while (true) {
                if ((map1 = scans[2].getNextMap(mid)) == null) {
                    break;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return new Pair(map1, mid);
                }
            }
            while (true) {
                if ((map1 = scans[3].getNextMap(mid)) == null) {
                    break;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return new Pair(map1, mid);
                }
            }
            while (true) {
                if ((map1 = scans[4].getNextMap(mid)) == null) {
                    return null;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return new Pair(map1, mid);
                }
            }
        }else{
            while (true) {
                if ((map1 = scan.getNextMap(mid)) == null) {
                    return null;
                }
                map1.setDefaultHdr();
                map1.setFldOffset(map1.getMapByteArray());
                if (PredEval.Eval(OutputFilter, map1, null, null, null) == true) {
                    return new Pair(map1, mid);
                }
            }
        }

    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     */
    public void close() {

        if (!closeFlag) {
            if(this.combined){
                scans[0].closescan();
                scans[1].closescan();
                scans[2].closescan();
                scans[3].closescan();
                scans[4].closescan();
            }else{
                scan.closescan();
            }
            closeFlag = true;
        }
    }

}


