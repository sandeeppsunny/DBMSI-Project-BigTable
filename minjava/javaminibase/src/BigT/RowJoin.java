package BigT;

import global.MapOrder;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;

public class RowJoin{
    private String bigTable1;
    private String bigTable2;
    private String outBigTable;
    private String columnFilter;
    private Heapfile tempHeapFile1;
    private Heapfile tempHeapFile2;
    private String heapFileName1;
    private String heapFileName2;
    private Stream stream1;
    private Stream stream2;
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
            this.numBuf = (int)((SystemDefs.JavabaseBM.getNumBuffers()*3)/8);
        }catch(Exception e){
            System.err.println("RowJoin.java: Error in getting the number of records in the bigtables");
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
        buildTempHeapFiles();
        System.out.println("Number of elements in temporary heap file1: " + this.tempHeapFile1.getRecCntMap());
        System.out.println("Number of elements in temporary heap file1: " + this.tempHeapFile2.getRecCntMap());
        ArrayList<String[]> matchingRowlabels =  getMatchingRowLabels();
        if(matchingRowlabels == null){
            System.err.println("NO MATCHING ROWS FOUND");
            return;
        }else{
            for(String[] temp: matchingRowlabels){
                System.out.println("Matching Rows: " + temp[0] + " -- " + temp[1]);
            }
            performJoin(matchingRowlabels);
        }
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
            try{
                this.stream1 = new Stream(this.bigTable1, null, 1, 1, "*",
                        this.columnFilter, "*", this.numBuf);
            }catch(Exception e){
                System.err.println("RowJoin.java: Error caused in opening stream on bigtable1");
                return;
            }
            Map prevMap=null;
            Map temp = new Map();
            temp = this.stream1.getNext();
            String rowLabel = "";
            if(temp!=null){
                temp.setFldOffset(temp.getMapByteArray());
                rowLabel = temp.getRowLabel();
                prevMap = new Map(temp);
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
                prevMap = new Map(temp);

            }
            if(prevMap!=null) {
                this.tempHeapFile1.insertRecordMap(prevMap.getMapByteArray());
            }
            this.stream1.closestream();

            // Creating tempHeapFile 2
            try{
                this.stream2 = new Stream(this.bigTable2, null, 1, 1, "*",
                        this.columnFilter, "*", this.numBuf);
            }catch(Exception e){
                System.err.println("RowJoin.java: Error caused in opening stream on bigtable2");
                return;
            }

            prevMap=new Map();
            rowLabel = "";
            temp = this.stream2.getNext();
            if(temp!=null){
                temp.setFldOffset(temp.getMapByteArray());
                rowLabel = temp.getRowLabel();
                prevMap = new Map(temp);
            }
            prevMap.setFldOffset(prevMap.getMapByteArray());
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
                prevMap = new Map(temp);

            }
            if(prevMap!=null) {
                this.tempHeapFile2.insertRecordMap(prevMap.getMapByteArray());
            }
            this.stream2.closestream();

        }catch(Exception e){
            System.err.println("RowJoin.java: Error caused in building temporary heapfiles");
        }
    }

    public ArrayList<String[]> getMatchingRowLabels() throws Exception {
        ArrayList<String[]> res = new ArrayList<>();
        String[] temp = new String[2];
        SortMap[] sortMaps = getSortedStreams();
        if(sortMaps!=null){
            Map mapLeft = null;
            Map mapRight = null;
            while(true){
                if((mapLeft = sortMaps[0].get_next()) == null){
                    break;
                }
                mapLeft.setFldOffset(mapLeft.getMapByteArray());
                while(true){
                    if((mapRight = sortMaps[1].get_next()) == null){
                        break;
                    }
                    mapRight.setFldOffset(mapRight.getMapByteArray());
                    if(mapLeft.getValue().compareTo(mapRight.getValue()) == 0){
                        temp = new String[2];
                        temp[0] = mapLeft.getRowLabel();
                        temp[1] = mapRight.getRowLabel();
                        res.add(temp);
                    }
                }
                sortMaps[1].close();
                FileScanMap fileScanMap2 = new FileScanMap(this.heapFileName2, null, null, false);
                sortMaps[1] = new SortMap(null, null, null, fileScanMap2,
                        6, new MapOrder(MapOrder.Ascending), null, this.numBuf);
            }
            sortMaps[0].close();
        }
        return res;
    }

    /**
     * Used to get the sortMap where sorting is done based on the values.
     * @return
     */
    public SortMap[] getSortedStreams(){
        SortMap[] res = null;
        try{
            FileScanMap fileScanMap1 = new FileScanMap(this.heapFileName1, null, null, false);
            FileScanMap fileScanMap2 = new FileScanMap(this.heapFileName2, null, null, false);
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

    public void performJoin(ArrayList<String[]> matchingRowLabels){
        bigt outBigT;
        try{
            outBigT = new bigt(this.outBigTable, true);
        }catch(Exception e){
            System.err.println("RowJoin.java: Exception thrown in creating output bigtable");
            e.printStackTrace();
            return;
        }
        Stream tempStream;
        try{
            for(String[] matchingRows: matchingRowLabels){
                tempStream = new Stream(this.bigTable1, null, 1, 1
                        , matchingRows[0], "*", "*", this.numBuf);
                Map tempMap = null;
                String tempColLabel;
                while(true){
                    if((tempMap = tempStream.getNext()) == null){
                        break;
                    }
                    tempMap.setFldOffset(tempMap.getMapByteArray());
                    tempMap.setRowLabel(matchingRows[0] + ":" + matchingRows[1]);
                    if(!tempMap.getColumnLabel().equals(this.columnFilter)){
                        tempColLabel = tempMap.getColumnLabel();
                        tempMap.setColumnLabel(matchingRows[0] + "_" + tempColLabel);
                    }
                    outBigT.insertMap(tempMap, 1);
                }
                tempStream.closestream();

                tempStream = new Stream(this.bigTable2, null, 1, 1
                        , matchingRows[1], "*", "*", this.numBuf);

                tempMap = null;
                tempColLabel = "";
                while(true){
                    if((tempMap = tempStream.getNext()) == null){
                        break;
                    }
                    tempMap.setFldOffset(tempMap.getMapByteArray());
                    tempMap.setRowLabel(matchingRows[0] + ":" + matchingRows[1]);
                    if(!tempMap.getColumnLabel().equals(this.columnFilter)){
                        tempColLabel = tempMap.getColumnLabel();
                        tempMap.setColumnLabel(matchingRows[1] + "_" + tempColLabel);
                    }
                    outBigT.insertMap(tempMap, 1);
                }
                tempStream.closestream();

            }
        }catch(Exception e){
            System.err.println("RowJoin.java: Exception thrown while inserting records into output bigtable");
            e.printStackTrace();
            return;
        }
        try{
            System.out.println("ROWJOIN SUCCESS: NUMBER OF RECORDS IN OUTPUT BIGTABLE: " + outBigT.getMapCnt());
        }catch(Exception e){
            System.err.println("RowJoin.java: Exception thrown in retrieving the number of records in output bigtable");
            e.printStackTrace();
            return;
        }
    }

    public void deleteDuplicateRecords(){

    }


}