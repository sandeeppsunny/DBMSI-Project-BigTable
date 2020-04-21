package BigT;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.MapOrder;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.HashSet;

public class GetAllCount{

    HashSet<String> allBigt;
    int numBuf;

    public GetAllCount(HashSet<String> allBigt, int numBuf){
        this.allBigt = allBigt;
        this.numBuf = (int)(numBuf*3)/4;
    }

    public void run() throws Exception {

        Stream tempStream;
        int rowCount;
        int colCount;
//        Heapfile rowLabels = new Heapfile("rowLabels");
//        Heapfile colLabels = new Heapfile("colLabels");
        bigt tempBigt;
        int mapCount = 0;
        for(String bigtName: allBigt){
            /*tempStream = new Stream(bigtName, null, 1, 3, "*",
                    "*", "*", this.numBuf);
            createHeapFile(tempStream, rowLabels, 3);

            tempStream = new Stream(bigtName, null, 1, 4, "*",
                    "*", "*", this.numBuf);
            createHeapFile(tempStream, colLabels, 4);

             */
            System.out.println("Bigtable: " + bigtName);
            System.out.println("---------------------------------------");
            tempBigt = new bigt(bigtName, false);
            mapCount = tempBigt.getMapCnt();
            rowCount = tempBigt.getRowCnt();
            colCount = tempBigt.getColumnCnt();

            System.out.println("TOTAL NUMBER OF MAPS: " + mapCount);
            System.out.println("NUMBER OF DISTINCT ROW LABELS: "+ rowCount);
            System.out.println("NUMBER OF DISTINCT COLUMN LABELS: "+ colCount);
            System.out.println("---------------------------------------");
        }

        /*int rowCount = getCounts(rowLabels, 3);

        int colCount = getCounts(colLabels, 4);

        System.out.println("TOTAL NUMBER OF BIGTABLES CREATED: " + allBigt.size());
        System.out.println("TOTAL NUMBER OF MAPS IN THE DATABASE: " + mapCount);
        System.out.println("TOTAL NUMBER OF DISTINCT ROW LABELS IN THE DATABASE: " + rowCount);
        System.out.println("TOTAL NUMBER OF DISTINCT COLUMN LABELS IN THE DATABASE: "+ colCount);

        rowLabels.deleteFileMap();
        colLabels.deleteFileMap();*/
    }

    public void createHeapFile(Stream stream, Heapfile heapfile, int type) throws IOException,
            InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException,
            InvalidSlotNumberException, HFDiskMgrException {
        Map tempMap = stream.getNext();
        String label = "";
        Map insertMap;
        while(tempMap!=null){
            tempMap.setFldOffset(tempMap.getMapByteArray());
            insertMap = new Map();
            insertMap.setDefaultHdr();
            if(type == 3){
                if(!tempMap.getRowLabel().equals(label)){
                    label = tempMap.getRowLabel();
                    insertMap.setRowLabel(label);
                    heapfile.insertRecordMap(insertMap.getMapByteArray());
                }
            }else{
                if(!tempMap.getColumnLabel().equals(label)){
                    label = tempMap.getColumnLabel();
                    insertMap.setColumnLabel(label);
                    heapfile.insertRecordMap(insertMap.getMapByteArray());
                }
            }

            tempMap = stream.getNext();
        }
        stream.closestream();
    }

    public int getCounts(Heapfile heapfile, int type) throws Exception {
        FileScanMap fileScanMap = new FileScanMap(heapfile._fileName, null, null, false);
        SortMap sortMap = new SortMap(null, null, null, fileScanMap,
                type, new MapOrder(MapOrder.Ascending), null, this.numBuf);
        int count = 0;
        String label = "";
        Map tempMap = sortMap.get_next();
        while(tempMap!=null){
            tempMap.setFldOffset(tempMap.getMapByteArray());
            if(type == 3){
                if(!tempMap.getRowLabel().equals(label)){
                    label = tempMap.getRowLabel();
                    count += 1;
                }
            }else{
                if(!tempMap.getColumnLabel().equals(label)){
                    label = tempMap.getColumnLabel();
                    count += 1;
                }
            }
            tempMap = sortMap.get_next();
        }
        sortMap.close();

        return count;
    }


}