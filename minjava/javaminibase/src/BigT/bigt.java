package BigT;

import java.io.*;
import java.util.Arrays;

import BigT.Map;
import diskmgr.*;
import bufmgr.*;
import global.*;
import btree.*;
import heap.*;

public class bigt extends Heapfile {

    private String name;
    // PageId _firstDirPageId;   // page number of header page
    // int _ftype;
    // private boolean _file_deleted;
    // private String _fileName;
    // private static int tempfilecount = 0;

    private BTreeFile index1 = null;
    private BTreeFile index2 = null;
    private int mapCount = 0;
    private int type;

    bigt(java.lang.String name, int type)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException,
            GetFileEntryException,
            ConstructPageException,
            AddFileEntryException {
        super(name);
        this.name = name;
        this.type = type;
        createIndex(type);
    }

    public void createIndex(int type) throws GetFileEntryException,
            ConstructPageException,
            IOException,
            AddFileEntryException {
        switch(type){
            case 1:
                break;
            case 2:
                index1 = new BTreeFile("Index1", AttrType.attrString, 20, DeleteFashion.NAIVE_DELETE);
                break;
            case 3:
                index1 = new BTreeFile("Index1", AttrType.attrString, 20, DeleteFashion.NAIVE_DELETE);
                break;
            case 4:
                index1 = new BTreeFile("Index1", AttrType.attrString, 40, DeleteFashion.NAIVE_DELETE);
                index2 = new BTreeFile("Index2", AttrType.attrInteger, 4, DeleteFashion.NAIVE_DELETE);
                break;
            case 5:
                index1 = new BTreeFile("Index1", AttrType.attrString, 40, DeleteFashion.NAIVE_DELETE);
                index2 = new BTreeFile("Index2", AttrType.attrInteger, 4, DeleteFashion.NAIVE_DELETE);
                break;
        }
    }

    public void insertIndex(RID rid, byte[] mapPtr) throws KeyTooLongException,
                KeyNotMatchException,
                LeafInsertRecException,
                IndexInsertRecException,
                ConstructPageException,
                UnpinPageException,
                PinPageException,
                NodeNotMatchException,
                ConvertException,
                DeleteRecException,
                IndexSearchException,
                IteratorException,
                LeafDeleteException,
                InsertException,
                IOException {
        Map map = new Map(mapPtr, 0, mapPtr.length);
        map.setFldOffset(mapPtr);
        switch(type){
            case 1:
                break;
            case 2:
                index1.insert(new StringKey(map.getRowLabel()), rid);
                break;
            case 3:
                index1.insert(new StringKey(map.getColumnLabel()), rid);
                break;
            case 4:
                index1.insert(new StringKey(map.getColumnLabel()+map.getRowLabel()), rid);
                index2.insert(new IntegerKey(map.getTimeStamp()), rid);
                break;
            case 5:
                index1.insert(new StringKey(map.getRowLabel()+map.getValue()), rid);
                index2.insert(new IntegerKey(map.getTimeStamp()), rid);
                break;
            }
        }

    public void deleteBigt() {

	}

	public int getMapCnt() {
		return this.mapCount;
	}

	public int getRowCnt() {
		return 0;
	}

	public int getColumnCnt() {
		return 0;
	}

	public RID insertMap(byte[] mapPtr) throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException,
            KeyTooLongException,
            KeyNotMatchException,
            LeafInsertRecException,
            IndexInsertRecException,
            ConstructPageException,
            UnpinPageException,
            PinPageException,
            NodeNotMatchException,
            ConvertException,
            DeleteRecException,
            IndexSearchException,
            IteratorException,
            LeafDeleteException,
            InsertException{
        this.mapCount++;
        RID rid = insertRecordMap(mapPtr);
        insertIndex(rid, mapPtr);
		return rid;
	}

	public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) {
		return null;
	}
}
