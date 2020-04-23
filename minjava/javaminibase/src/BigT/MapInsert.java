package BigT;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import heap.*;

import java.io.IOException;

public class MapInsert{
    String RowLabel,ColumnLabel,Value,TableName;
    int TimeStamp, Type;
    bigt Table;
    Map map;

    public MapInsert(String RowLabel, String ColumnLabel, String Value, int TimeStamp, int Type, String TableName){
        this.RowLabel = RowLabel;
        this.ColumnLabel = ColumnLabel;
        this.Value = Value;
        this.TimeStamp = TimeStamp;
        this.Type = Type;
        this.TableName = TableName;
        for(int j=Value.length(); j < Map.DEFAULT_STRING_ATTRIBUTE_SIZE; j++){
            this.Value = "0"+this.Value;
        }
    }

    public void run()
            throws IOException, InvalidTupleSizeException, UnpinPageException,
            HFBufMgrException, ReplacerException, HFException, PageUnpinnedException,
            HashEntryNotFoundException, HFDiskMgrException, PinPageException, FreePageException,
            GetFileEntryException, DeleteFileEntryException, AddFileEntryException, ConstructPageException,
            IteratorException, InvalidFrameNumberException, InvalidSlotNumberException {
        long startTime, endTime;
        startTime = System.nanoTime();
        map = new Map();
        map.setDefaultHdr();
        map.setRowLabel(this.RowLabel);
        map.setColumnLabel(this.ColumnLabel);
        map.setValue(this.Value);
        map.setTimeStamp(this.TimeStamp);
        this.Table = new bigt(this.TableName, false);
        try{
            this.Table.insertMapIntoAlreadySortedFile(map, this.Type);
        }catch(Exception ex){
            System.err.println("MapInsert.java: Exception in inserting map");
            ex.printStackTrace();
        }
        endTime = System.nanoTime();

        System.out.println("TIME TAKEN TO INSERT MAP " + ((endTime - startTime)/1000000000) + " s");
        startTime = System.nanoTime();
        try {
            this.Table.indexCreateUtil();
            this.Table.duplicateRecordsMapInsert(this.TableName, this.map);
        } catch (Exception e) {
            System.err.println("MapInsert.java: Exception caused in deleting duplicate records");
            e.printStackTrace();
        }
        endTime = System.nanoTime();
        System.out.println("TIME TAKEN FOR DUPLICATE RECORDS REMOVAL "+((endTime - startTime)/1000000000) + " s");

        startTime = System.nanoTime();
        try{
            if(this.Type != 1) {
                this.Table.deleteAllNodesInIndex(this.Table.indexFiles.get(this.Type));
            }
        }catch(Exception e){
            System.err.println("Exception caused in deleting entries in the main index of storage type: " + this.Type);
        }
        this.Table.createMapInsertIndex(this.Type);
        endTime = System.nanoTime();
        System.out.println("TIME TAKEN FOR CREATING MAIN INDICES "+((endTime - startTime)/1000000000) + " s");
        System.out.println();

        System.out.println("NUMBER OF MAPS IN THE CURRENT BIGTABLE: " + this.Table.getMapCnt());

    }
}