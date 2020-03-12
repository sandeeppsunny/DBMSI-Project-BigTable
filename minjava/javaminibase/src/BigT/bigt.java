package BigT;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import BigT.Map;
import diskmgr.*;
import bufmgr.*;
import global.*;
import btree.*;
import heap.*;

public class bigt {

    private String name;
    // PageId _firstDirPageId; // page number of header page
    // int _ftype;
    // private boolean _file_deleted;
    // private String _fileName;
    // private static int tempfilecount = 0;

    private Heapfile _hf;
    private BTreeFile index1 = null;
    private BTreeFile index2 = null;
    private int type;
    public HashMap<String, ArrayList<RID>> hashMap = new HashMap<String, ArrayList<RID>>();

    public bigt(java.lang.String name, int type) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
            GetFileEntryException, ConstructPageException, AddFileEntryException {
        _hf = new Heapfile(name);
        this.name = name;
        this.type = type;
        createIndex();
    }

    public Heapfile getheapfile() {
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

    public void insertIndex(RID rid, Map map) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
            IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
            NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException, IOException {
        switch (type) {
            case 1:
                break;
            case 2:
                index1.insert(new StringKey(map.getRowLabel()), rid);
                break;
            case 3:
                index1.insert(new StringKey(map.getColumnLabel()), rid);
                break;
            case 4:
                index1.insert(new StringKey(map.getColumnLabel() + map.getRowLabel()), rid);
                index2.insert(new IntegerKey(map.getTimeStamp()), rid);
                break;
            case 5:
                index1.insert(new StringKey(map.getRowLabel() + map.getValue()), rid);
                index2.insert(new IntegerKey(map.getTimeStamp()), rid);
                break;
        }
    }

    public void removeIndex(RID rid, Map map)
            throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
            ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
            DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
            IOException, DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
            FreePageException, RecordNotFoundException, IndexFullDeleteException {
        switch (type) {
            case 1:
                break;
            case 2:
                index1.Delete(new StringKey(map.getRowLabel()), rid);
                break;
            case 3:
                index1.Delete(new StringKey(map.getColumnLabel()), rid);
                break;
            case 4:
                index1.Delete(new StringKey(map.getColumnLabel() + map.getRowLabel()), rid);
                index2.Delete(new IntegerKey(map.getTimeStamp()), rid);
                break;
            case 5:
                index1.Delete(new StringKey(map.getRowLabel() + map.getValue()), rid);
                index2.Delete(new IntegerKey(map.getTimeStamp()), rid);
                break;
        }
    }

    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        return _hf.getRecCntMap();
    }

    public int getRowCnt() {
        return 0;
    }

    public int getColumnCnt() {
        return 0;
    }

    public RID insertMap(Map map) throws DeleteFashionException, LeafRedistributeException, RedistributeException,
            InsertRecException, FreePageException, RecordNotFoundException, IndexFullDeleteException, Exception {
        RID rid = new RID();
        ArrayList<RID> rids = hashMap.get(map.getRowLabel() + map.getColumnLabel());
        if (rids == null) {
            rids = new ArrayList<RID>();
        }
        if (rids.size() == 3) {
            rid = rids.get(0);
            Map temp = _hf.getRecordMap(rid);
            temp.setFldOffset(temp.getMapByteArray());
            removeIndex(rid, temp);
            _hf.deleteRecordMap(rid);
        }
        rid = _hf.insertRecordMap(map.getMapByteArray());
        insertIndex(rid, map);
        rids.add(rid);
        hashMap.put(map.getRowLabel() + map.getColumnLabel(), rids);
        return rid;
    }

    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) {
        return null;
    }
}
