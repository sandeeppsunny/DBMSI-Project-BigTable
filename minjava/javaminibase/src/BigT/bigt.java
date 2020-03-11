package BigT;

import java.io.*;
import java.util.Arrays;

import BigT.Map;
import diskmgr.*;
import bufmgr.*;
import global.*;
import btree.*;
import heap.*;

public class bigt{

    private String name;
    // PageId _firstDirPageId;   // page number of header page
    // int _ftype;
    // private boolean _file_deleted;
    // private String _fileName;
    // private static int tempfilecount = 0;

    private Heapfile _hf;
    private BTreeFile index1 = null;
    private BTreeFile index2 = null;
    private int type;

    public bigt(java.lang.String name, int type)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException,
            GetFileEntryException,
            ConstructPageException,
            AddFileEntryException {
        _hf = new Heapfile(name);
        this.name = name;
        this.type = type;
    }

    public Heapfile getheapfile(){
        return _hf;
    }

    public void createIndex(String indexName1, String indexName2) throws GetFileEntryException,
            ConstructPageException,
            IOException,
            AddFileEntryException {
        switch(type){
            case 1:
                break;
            case 2:
                index1 = new BTreeFile(indexName1, AttrType.attrString, 20, DeleteFashion.FULL_DELETE);
                break;
            case 3:
                index1 = new BTreeFile(indexName1, AttrType.attrString, 20, DeleteFashion.FULL_DELETE);
                break;
            case 4:
                index1 = new BTreeFile(indexName1, AttrType.attrString, 40, DeleteFashion.FULL_DELETE);
                index2 = new BTreeFile(indexName2, AttrType.attrInteger, 4, DeleteFashion.FULL_DELETE);
                break;
            case 5:
                index1 = new BTreeFile(indexName1, AttrType.attrString, 40, DeleteFashion.FULL_DELETE);
                index2 = new BTreeFile(indexName2, AttrType.attrInteger, 4, DeleteFashion.FULL_DELETE);
                break;
        }
    }

    public void insertIndex(RID rid, Map map) throws KeyTooLongException,
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

	public int getMapCnt() throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException {
		return _hf.getRecCntMap();
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
        RID rid = _hf.insertRecordMap(mapPtr);
		return rid;
	}

	public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) {
		return null;
	}
}
