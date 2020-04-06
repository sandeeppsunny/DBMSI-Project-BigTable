package BigT;

import global.MapOrder;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import iterator.FileScanMap;
import iterator.MapUtils;
import iterator.SortMap;

import java.io.IOException;

public class RowJoin{
    private String bigTable1;
    private String bigTable2;
    private String outBigTable;
    private String columnFilter;
    private Heapfile tempHeapFile1;
    private Heapfile tempHeapFile2;
    private String heapFileName1;
    private String heapFileName2;
    private CombinedStream stream1;
    private CombinedStream stream2;
    private bigt bigt1;
    private bigt bigt2;
    private int numBuf;

    public RowJoin(String bigTable1, String bigTable2, String outBigTable, String columnFilter){
        this.bigTable1 = bigTable1;
        this.bigTable2 = bigTable2;
        this.outBigTable = outBigTable;
        this.columnFilter = columnFilter;
        this.heapFileName1 = this.bigTable1 + "_jointemp";
        this.heapFileName2 = this.bigTable2 + "_jointemp";
        try{
            this.bigt1 = new bigt(this.bigTable1, false);
            this.bigt2 = new bigt(this.bigTable2, false);
        }catch(Exception e){
            System.err.println("RowJoin.java: Error caused in retrieving the bigtables");
            e.printStackTrace();
        }
        try{
            System.out.println("Records in bigt1: " + this.bigt1.getMapCnt());
            System.out.println("Records in bigt2: " + this.bigt2.getMapCnt());
            this.numBuf = SystemDefs.JavabaseBM.getNumBuffers();
            this.stream1 = new CombinedStream(this.bigt1, 1, "*",
                    this.columnFilter, "*", this.numBuf);
            this.stream2 = new CombinedStream(this.bigt2, 1, "*",
                    this.columnFilter, "*", this.numBuf);
        }catch(Exception e){
            System.err.println("RowJoin.java: Error in getting the combined stream");
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
        buildTempHeapFiles();
        String[] matchingRowlabels =  getMatchingRowLabels();
        if(matchingRowlabels == null){
            System.err.println("NO MATCHING ROWS FOUND");
            return;
        }else{
            System.out.println("Matching Rows: " + matchingRowlabels[0] + " -- " + matchingRowlabels[1]);
        }
        createOutputBigTable();
    }

    public void buildTempHeapFiles(){
        try{
            this.tempHeapFile1 = new Heapfile(this.heapFileName1);
            this.tempHeapFile2 = new Heapfile(this.heapFileName2);
        }catch(Exception e){
            System.err.println("RowJoin.java: Error caused in creating temporary heapfiles");
        }
        try {
            // Creating tempHeapFile 1
            Map prevMap=null;
            Map temp = this.stream1.getNext();
            String rowLabel = "";
            if(temp!=null){
                temp.setFldOffset(temp.getMapByteArray());
                rowLabel = temp.getRowLabel();
                prevMap = temp;
            }
            while (true) {
                temp = this.stream1.getNext();
                if (temp == null) {
                    break;
                }
                temp.setFldOffset(temp.getMapByteArray());
                if(!temp.getRowLabel().equals(rowLabel)){
                    this.tempHeapFile1.insertRecordMap(prevMap.getMapByteArray());
                    rowLabel = temp.getRowLabel();
                }
                prevMap = temp;

            }
            if(prevMap!=null) {
                this.tempHeapFile1.insertRecordMap(prevMap.getMapByteArray());
            }

            // Creating tempHeapFile 2
            prevMap=null;
            rowLabel = "";
            temp = this.stream2.getNext();
            if(temp!=null){
                temp.setFldOffset(temp.getMapByteArray());
                rowLabel = temp.getRowLabel();
                prevMap = temp;
            }
            while (true) {
                temp = this.stream2.getNext();
                if (temp == null) {
                    break;
                }
                temp.setFldOffset(temp.getMapByteArray());
                if(!temp.getRowLabel().equals(rowLabel)){
                    this.tempHeapFile2.insertRecordMap(prevMap.getMapByteArray());
                    rowLabel = temp.getRowLabel();
                }
                prevMap = temp;

            }
            if(prevMap!=null) {
                this.tempHeapFile2.insertRecordMap(prevMap.getMapByteArray());
            }
            this.stream1.closeCombinedStream();
            this.stream2.closeCombinedStream();

        }catch(Exception e){
            System.err.println("RowJoin.java: Error caused in building temporary heapfiles");
        }
    }

    public String[] getMatchingRowLabels() throws Exception {
        String[] res = null;
        SortMap[] sortMaps = getSortedStreams();
        if(sortMaps!=null){
            Map mapLeft = sortMaps[0].get_next();
            Map mapRight = sortMaps[1].get_next();
            if(mapLeft!=null){
                mapLeft.setFldOffset(mapLeft.getMapByteArray());
            }
            if(mapRight!=null){
                mapRight.setFldOffset(mapRight.getMapByteArray());
            }
            while(true){
                if(mapLeft.getValue().compareTo(mapRight.getValue()) < 0){
                    mapLeft = sortMaps[0].get_next();
                }else if(mapLeft.getValue().compareTo(mapRight.getValue()) > 0){
                    mapRight = sortMaps[1].get_next();
                }else{
                    res = new String[2];
                    res[0] = mapLeft.getValue();
                    res[1] = mapRight.getValue();
                    break;
                }

                if(mapLeft == null || mapRight == null){
                    break;
                }
                else{
                    mapLeft.setFldOffset(mapLeft.getMapByteArray());
                    mapRight.setFldOffset(mapRight.getMapByteArray());
                }
            }
        }
        return res;
    }

    public SortMap[] getSortedStreams(){
        SortMap[] res = null;
        try{
            FileScanMap fileScanMap1 = new FileScanMap(this.heapFileName1, null, null);
            FileScanMap fileScanMap2 = new FileScanMap(this.heapFileName2, null, null);
            res  = new SortMap[2];
            res[0] = new SortMap(null, null, null, fileScanMap1,
                    6, new MapOrder(MapOrder.Ascending), null, this.numBuf);
            res[1] = new SortMap(null, null, null, fileScanMap2,
                    6, new MapOrder(MapOrder.Ascending), null, this.numBuf);
        }catch(Exception e){
            System.err.println("RowJoin.java: getSortedStreams(): Exception caused in initializing sort iterator on temporary heap files");
            e.printStackTrace();
        }
        return res;
    }

    public void createOutputBigTable(){

    }


}