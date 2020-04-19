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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RowSort{


    private bigt resultTable;
    private int numBuf;
    private bigt sourceTable;
    private String sourceTableName;
    private String ColumnName;
    private MapOrder mapOrder;

    public RowSort(String sourceTableName, String resultTable, int rowOrder, String ColumnName, int n_pages)
        throws Exception{
        this.numBuf = n_pages;
        this.sourceTableName = sourceTableName;
        this.ColumnName = ColumnName;
        this.sourceTable = new bigt(sourceTableName, false);
        this.resultTable = new bigt(resultTable, false);

        if(rowOrder == 1){
            mapOrder = new MapOrder(MapOrder.Ascending);
        }else if(rowOrder == 2){
            mapOrder = new MapOrder(MapOrder.Descending);
        }
    }

    public void run() throws Exception {
        //create heap file with all rows recent timestamps sorted based on row + timestamp
        Stream stream1 = new Stream(sourceTableName, null, 1, 3, "*",
                "*", "*", numBuf);
        Heapfile rows = new Heapfile("rows");
        createHeap(stream1, rows);
        stream1.closestream();

        Stream stream2 = new Stream(sourceTableName, null, 1, 3, "*",
                ColumnName, "*", numBuf);
        Heapfile filterRows = new Heapfile("filterRows");
        createHeap(stream2, filterRows);
        stream2.closestream();

        dumpDefault(rows, filterRows);

        //Sort iterator on filterRows heapfile to insert the filterRows in bigt -> sort order 7 (value based)
        FileScanMap tempScan = new FileScanMap("filterRows", null, null, false);
        SortMap sort = new SortMap(null, null, null, tempScan, 7, mapOrder,
                null, numBuf);

        Map mapSort = sort.get_next();
        String rowLabel;

        while(mapSort!=null){
            mapSort.setDefaultHdr();
            rowLabel = mapSort.getRowLabel();
            FileScanMap rowLabelScan = new FileScanMap(sourceTableName, null,
                    getConditionalExpression(rowLabel), true);
            Map tempMap = rowLabelScan.get_next();
            while(tempMap!=null){
                tempMap.setDefaultHdr();
                resultTable.insertMap(tempMap, 1);
                tempMap = rowLabelScan.get_next();
            }
            rowLabelScan.close();
            mapSort = sort.get_next();
        }
        sort.close();
        System.out.println("NUMBER OF MAPS IN OUTPUT BIGTABLE: " + resultTable.getMapCnt());

        rows.deleteFileMap();
        filterRows.deleteFileMap();
    }

    public void createHeap(Stream stream, Heapfile heapfile) throws IOException, InvalidTupleSizeException,
            SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        Map tempMap = null;
        tempMap = stream.getNext();
        Map prevMap = null;
        if(tempMap!=null){
            tempMap.setDefaultHdr();
            prevMap = new Map(tempMap);
        }
        while(tempMap!=null){
            tempMap.setDefaultHdr();
            if(!tempMap.getRowLabel().equals(prevMap.getRowLabel())){
                heapfile.insertRecordMap(prevMap.getMapByteArray());
            }
            prevMap = new Map(tempMap);
            tempMap = stream.getNext();
        }
        heapfile.insertRecordMap(prevMap.getMapByteArray());
    }

    public void dumpDefault(Heapfile allRows, Heapfile filterRows) throws InvalidTupleSizeException, IOException,
            FileScanException, TupleUtilsException, InvalidRelation, HFBufMgrException, HFException,
            FieldNumberOutOfBoundException, InvalidSlotNumberException, SpaceNotAvailableException, HFDiskMgrException,
            JoinsException, PageNotReadException, WrongPermat, InvalidTypeException, PredEvalException, UnknowAttrType {
        Scan sc1 = allRows.openScanMap();
        Scan sc2 = filterRows.openScanMap();

        MID mid_left = new MID();
        MID mid_right = new MID();
        CondExpr[] expr;

        String rowLabel1, rowLabel2;
        Map map_left = sc1.getNextMap(mid_left);
        Map map_right = sc2.getNextMap(mid_right);
        while(map_left!=null){
            map_left.setDefaultHdr();
            rowLabel1 = map_left.getRowLabel();
            if(map_right!=null){
                map_right.setDefaultHdr();
                rowLabel2 = map_right.getRowLabel();
            }else{
                rowLabel2 = "";
            }
            if(!rowLabel1.equals(rowLabel2)){
                expr = getConditionalExpression(rowLabel1);
                FileScanMap tempFileScan = new FileScanMap(sourceTableName, null, expr, true);
                Map tempMap = tempFileScan.get_next();
                while(tempMap!=null){
                    tempMap.setDefaultHdr();
                    resultTable.insertMap(tempMap, 1);
                    tempMap = tempFileScan.get_next();
                }
                tempFileScan.close();
            }else{
                map_right = sc2.getNextMap(mid_right);
            }

            map_left = sc1.getNextMap(mid_left);
        }
        sc1.closescan();
        sc2.closescan();
    }



    public CondExpr[] getConditionalExpression(String rowLabel){
        CondExpr[] res = new CondExpr[2];
        CondExpr expr = new CondExpr();
        expr.op = new AttrOperator(AttrOperator.aopEQ);
        expr.type1 = new AttrType(AttrType.attrSymbol);
        expr.type2 = new AttrType(AttrType.attrString);
        expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        expr.operand2.string = rowLabel;
        expr.next = null;
        res[0] = expr;
        res[1] = null;

        return res;
    }
}
