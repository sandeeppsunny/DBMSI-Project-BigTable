package BigT;

import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import index.IndexException;
import index.MapIndexScan;
import index.UnknownIndexTypeException;
import iterator.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
    private bigt outBigT;
    private ArrayList<Map> duplicateMaps;

    public RowJoin(String bigTable1, String bigTable2, String outBigTable, String columnFilter){
        this.bigTable1 = bigTable1;
        this.bigTable2 = bigTable2;
        this.outBigTable = outBigTable;
        this.columnFilter = columnFilter;
        this.heapFileName1 = "jointemp_1";
        this.heapFileName2 = "jointemp_2";
        this.duplicateMaps = new ArrayList<>();
        try{
            this.bigt1 = new bigt(this.bigTable1, false);
            this.bigt2 = new bigt(this.bigTable2, false);
        }catch(Exception e){
            System.err.println("RowJoin.java: Error caused in retrieving the bigtables");
            e.printStackTrace();
        }
        try{
            System.out.println("Records in outer relation: " + this.bigt1.getMapCnt());
            System.out.println("Records in inner relation: " + this.bigt2.getMapCnt());
            this.numBuf = (int)((SystemDefs.JavabaseBM.getNumBuffers()*3)/4);
        }catch(Exception e){
            System.err.println("RowJoin.java: Error in getting the number of records in the bigtables");
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
        buildTempHeapFiles();
//        System.out.println("Number of elements in temporary heap file1: " + this.tempHeapFile1.getRecCntMap());
//        System.out.println("Number of elements in temporary heap file2: " + this.tempHeapFile2.getRecCntMap());
//        ArrayList<String[]> matchingRowlabels =  getMatchingRowLabels();
        Heapfile matchingRowlabels = getMatchingRowLabels();
        if(matchingRowlabels == null || matchingRowlabels.getRecCntMap() == 0){
            System.err.println("NO MATCHING ROWS FOUND");
            return;
        }else{
            /*for(String[] temp: matchingRowlabels){
                System.out.println("Matching Rows: " + temp[0] + " -- " + temp[1]);
            }*/
            System.out.println("Matching Row labels: " + matchingRowlabels.getRecCntMap());
            performJoin(matchingRowlabels);
            System.out.println("NUMBER OF MAPS IN THE OUTPUT BIGTABLE AFTER COMPLETING ROWJOIN: " + this.outBigT.getMapCnt());
        }
        matchingRowlabels.deleteFileMap();
        tempHeapFile1.deleteFileMap();
        tempHeapFile2.deleteFileMap();
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

    public Heapfile getMatchingRowLabels() throws Exception {
        Heapfile matchingRowLabel = new Heapfile(null);
//        String[] temp = new String[2];
        Map tempMap;
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
//                        temp = new String[2];
                        tempMap = new Map();
                        tempMap.setDefaultHdr();
                        tempMap.setRowLabel(mapLeft.getRowLabel() + ":" + mapRight.getRowLabel());
//                        temp[0] = mapLeft.getRowLabel();
//                        temp[1] = mapRight.getRowLabel();
                        matchingRowLabel.insertRecordMap(tempMap.getMapByteArray());
//                        res.add(temp);
                    }
                }
                sortMaps[1].close();
                FileScanMap fileScanMap2 = new FileScanMap(this.heapFileName2, null, null, false);
                sortMaps[1] = new SortMap(null, null, null, fileScanMap2,
                        1, new MapOrder(MapOrder.Ascending), null, (int)this.numBuf/2);
            }
            sortMaps[0].close();
            sortMaps[1].close();
        }
        return matchingRowLabel;
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
                    1, new MapOrder(MapOrder.Ascending), null, (int)this.numBuf/2);
            res[1] = new SortMap(null, null, null, fileScanMap2,
                    1, new MapOrder(MapOrder.Ascending), null, (int)this.numBuf/2);
        }catch(Exception e){
            System.err.println("RowJoin.java: getSortedStreams(): Exception caused in initializing sort iterator on temporary heap files");
            e.printStackTrace();
        }
        return res;
    }

    public void performJoin(Heapfile matchingRowLabels){

        try{
            SystemDefs.JavabaseBM.flushAllPagesForcibly();
            outBigT = new bigt(this.outBigTable, true);
        }catch(Exception e){
            System.err.println("RowJoin.java: Exception thrown in creating output bigtable");
            e.printStackTrace();
            return;
        }
        Stream tempStream;
        try{
            String[] matchingRows;
            FileScanMap matchingRowsScan = new FileScanMap(matchingRowLabels._fileName, null, null, false);
            Map rowLabels;
            while((rowLabels = matchingRowsScan.get_next())!=null){
                duplicateMaps = new ArrayList<>();
                rowLabels.setDefaultHdr();
                matchingRows = rowLabels.getRowLabel().split(":");
                tempStream = new Stream(this.bigTable1, null, 1, 1
                        , matchingRows[0], "*", "*", this.numBuf);
                Map tempMap = null;
                String tempColLabel;
                tempMap = tempStream.getNext();
                while(tempMap!=null){
                    tempMap.setFldOffset(tempMap.getMapByteArray());
                    tempMap = new Map(tempMap);
                    tempMap.setRowLabel(matchingRows[0] + ":" + matchingRows[1]);
                    if(!tempMap.getColumnLabel().equals(this.columnFilter)){
                        tempColLabel = tempMap.getColumnLabel();
                        tempMap.setColumnLabel(tempColLabel + "_" + "left");
                        outBigT.insertMap(tempMap, 1);
                    }else{
                        duplicateMaps.add(tempMap);
                    }
                    tempMap = tempStream.getNext();
                }
                tempStream.closestream();

                tempStream = new Stream(this.bigTable2, null, 1, 1
                        , matchingRows[1], "*", "*", this.numBuf);

                tempMap = null;
                tempColLabel = "";
                tempMap = tempStream.getNext();
                while(tempMap!=null){
                    tempMap.setFldOffset(tempMap.getMapByteArray());
                    tempMap = new Map(tempMap);
                    tempMap.setRowLabel(matchingRows[0] + ":" + matchingRows[1]);
                    if(!tempMap.getColumnLabel().equals(this.columnFilter)){
                        tempColLabel = tempMap.getColumnLabel();
                        tempMap.setColumnLabel(tempColLabel + "_" + "right");
                        outBigT.insertMap(tempMap, 1);
                    }else{
                        duplicateMaps.add(tempMap);
                    }
                    tempMap = tempStream.getNext();

                }
                tempStream.closestream();

                try{
                    deleteDuplicateRecords();
                }catch(Exception e){
                    System.err.println("RowJoin.java: Exception thrown in deleting duplicate records");
                    e.printStackTrace();
                }

            }
        }catch(Exception e){
            System.err.println("RowJoin.java: Exception thrown while inserting records into output bigtable");
            e.printStackTrace();
            return;
        }

    }

    public void deleteDuplicateRecords() throws InvalidTupleSizeException, HFException, IOException,
            FieldNumberOutOfBoundException, InvalidSlotNumberException, SpaceNotAvailableException,
            HFBufMgrException, HFDiskMgrException {

        if(duplicateMaps.size() <= 3){
            for(Map map: duplicateMaps){
                outBigT.insertMap(map, 1);
            }
        }else{
            Collections.sort(duplicateMaps, new Comparator<Map>() {
                @Override
                public int compare(Map o1, Map o2) {
                    try {
                        Integer o1String = o1.getTimeStamp();
                        Integer o2String = o2.getTimeStamp();
                        return o1String.compareTo(o2String);
                    } catch (IOException e) {
                        System.out.println("Exception caused in comparing duplicate maps time stamps");
                        e.printStackTrace();
                    }
                    return 0;
                }
            });
            int count = 0;
            int i = duplicateMaps.size()-1;
            while(count < 3){
                outBigT.insertMap(duplicateMaps.get(i), 1);
                i-=1;
                count+=1;
            }
        }
    }

    public CondExpr[] getConditionalExpression(){
        CondExpr[] res = new CondExpr[2];
        CondExpr expr = new CondExpr();
        expr.op = new AttrOperator(AttrOperator.aopEQ);
        expr.type1 = new AttrType(AttrType.attrSymbol);
        expr.type2 = new AttrType(AttrType.attrString);
        expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        expr.operand2.string = this.columnFilter;
        expr.next = null;
        res[0] = expr;
        res[1] = null;

        return res;
    }


}