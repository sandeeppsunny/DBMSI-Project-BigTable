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
import iterator.*;
import index.*;
import java.util.*;

public class bigt {

    private String name;

    private Heapfile _hf;
    private BTreeFile index1 = null;
    private BTreeFile index2 = null;
    private BTreeFile utilityIndex = null;
    public String indexName1;
    public String indexName2;
    public String indexUtil;
    public boolean insertBatch;
    private int type;
    AttrType[] attrType;
    FldSpec[] projlist;
    CondExpr[] expr;
    MapIndexScan iscan;
    short[] res_str_sizes = new short[]{Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};


    public bigt(java.lang.String name, int type, boolean insert) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
            GetFileEntryException, ConstructPageException, AddFileEntryException,btree.IteratorException,
            btree.UnpinPageException, btree.FreePageException, btree.DeleteFileEntryException, btree.PinPageException {
        String file_name = name+type;
        _hf = new Heapfile(file_name);
        this.name = file_name;
        this.type = type;
        indexUtil = file_name + "_" + "indexUtil";
        indexName1 = file_name + "_" +"index1";
        indexName2 = file_name + "_" + "index2";

        if(insert){
            //For multiple batch insert
            createIndex(indexName1, indexName2);
            createIndexUtil();
            if(index1 != null){
                index1.destroyFile();
            }
            if(index2 != null){
                index2.destroyFile();
            }
            if(utilityIndex != null){
                utilityIndex.destroyFile();
            }
            createIndex(indexName1, indexName2);
            createIndexUtil();
        }
        initCondExprs();
    }

    public void initCondExprs(){
        attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrString);
        projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 0);
        projlist[1] = new FldSpec(rel, 1);
        projlist[2] = new FldSpec(rel, 2);
        projlist[3] = new FldSpec(rel, 3);

        expr = new CondExpr[3];
        expr[0] = new CondExpr();
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrString);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        expr[0].operand2.string = "";
        expr[0].next = null;
        expr[1] = new CondExpr();
        expr[1].op = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        expr[1].operand2.string = "";
        expr[1].next = null;
        expr[2] = null;
    }

    public String getName(){
        return name;
    }

    public int getType() {
        return type;
    }

    public Heapfile getheapfile() {
        return _hf;
    }

    public void createIndex(String indexName1, String indexName2) throws GetFileEntryException,
            ConstructPageException,
            IOException,
            AddFileEntryException,
            btree.IteratorException,
            btree.UnpinPageException,
            btree.FreePageException,
            btree.DeleteFileEntryException,
            btree.PinPageException {
        switch(type){
            case 1:
                break;
            case 2:
                index1 = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, DeleteFashion.FULL_DELETE);
                break;
            case 3:
                index1 = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, DeleteFashion.FULL_DELETE);
                break;
            case 4:
                index1 = new BTreeFile(indexName1, AttrType.attrString, 2*Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5, DeleteFashion.FULL_DELETE);
                index2 = new BTreeFile(indexName2, AttrType.attrInteger, 4, DeleteFashion.FULL_DELETE);
                break;
            case 5:
                index1 = new BTreeFile(indexName1, AttrType.attrString, 2*Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5, DeleteFashion.FULL_DELETE);
                index2 = new BTreeFile(indexName2, AttrType.attrInteger, 4, DeleteFashion.FULL_DELETE);
                break;
        }
    }

    public void createIndexUtil(){
        try {
            utilityIndex = new BTreeFile(indexUtil, AttrType.attrString, 2*Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5, DeleteFashion.FULL_DELETE);
        }catch(Exception ex){
            System.err.println("Error in creating utility index");
            ex.printStackTrace();
        }
    }

    public void insertIndex(MID mid, Map map) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
            IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
            NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException, IOException {
        switch (type) {
            case 1:
                break;
            case 2:
                index1.insert(new StringKey(map.getRowLabel()), mid);
                break;
            case 3:
                index1.insert(new StringKey(map.getColumnLabel()), mid);
                break;
            case 4:
                index1.insert(new StringKey(map.getColumnLabel() + "%" + map.getRowLabel()), mid);
                index2.insert(new IntegerKey(map.getTimeStamp()), mid);
                break;
            case 5:
                index1.insert(new StringKey(map.getRowLabel() + "%" + map.getValue()), mid);
                index2.insert(new IntegerKey(map.getTimeStamp()), mid);
                break;
        }
    }

    public void removeIndex(MID mid, Map map)
            throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
            ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
            DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
            IOException, DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
            FreePageException, RecordNotFoundException, IndexFullDeleteException {
        switch (type) {
            case 1:
                break;
            case 2:
                index1.Delete(new StringKey(map.getRowLabel()), mid);
                break;
            case 3:
                index1.Delete(new StringKey(map.getColumnLabel()), mid);
                break;
            case 4:
                index1.Delete(new StringKey(map.getColumnLabel() + "%" + map.getRowLabel()), mid);
                index2.Delete(new IntegerKey(map.getTimeStamp()), mid);
                break;
            case 5:
                index1.Delete(new StringKey(map.getRowLabel() + "%" + map.getValue()), mid);
                index2.Delete(new IntegerKey(map.getTimeStamp()), mid);
                break;
        }
    }

    public void insertIndexUtil(MID mid, Map map)
            throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
            ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
            DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
            IOException, DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
            FreePageException, RecordNotFoundException, IndexFullDeleteException {
        /*if(insertBatch){

        }*/
//        System.out.println("Key length " + (map.getRowLabel() + map.getColumnLabel()).length());
//        System.out.println("Index key is " + (new StringKey(map.getRowLabel() + map.getColumnLabel())));
        utilityIndex.insert(new StringKey(map.getRowLabel() + map.getColumnLabel()), mid);
    }

    public void removeIndexUtil(MID mid, Map map)
            throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
            ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
            DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
            IOException, DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
            FreePageException, RecordNotFoundException, IndexFullDeleteException{
        utilityIndex.Delete(new StringKey(map.getColumnLabel() + map.getRowLabel()), mid);
    }

    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        return _hf.getRecCntMap();
    }

    public int getRowCnt()  throws Exception{
        return getCount(3);
    }

    public int getColumnCnt()  throws Exception{
        return getCount(4);
    }

    public int getCount(int orderType) throws Exception{
        int numBuf = (int)((SystemDefs.JavabaseBM.getNumBuffers()*3)/4);
        Stream stream = openStream(orderType,"*","*","*",numBuf);
        Map t = stream.getNext();
        int count = 0;
        String temp = "\0";
        while(t != null) {
            t.setFldOffset(t.getMapByteArray());
            if(orderType==3){
                if(!t.getRowLabel().equals(temp)){
                    temp = t.getRowLabel();
                    count++;
                }
            }else{
                if(!t.getColumnLabel().equals(temp)){
                    temp = t.getColumnLabel();
                    count++;
                }
            }
            t = stream.getNext();
        }
        stream.closestream();
        return count;
    }


    public MID insertMap(Map map) throws  Exception {
        MID mid = _hf.insertRecordMap(map.getMapByteArray());
        return mid;
    }

    public void buildUtilityIndex(){
        try{
//            System.out.println("Building Utility INdex");
            FileScanMap fscan = new FileScanMap(getName(), null, null);
            Pair mapPair;
            mapPair = fscan.get_next_mid();
            while(mapPair!=null){
//                mapPair.getMap().print();
                insertIndexUtil(mapPair.getRid(), mapPair.getMap());
                mapPair = fscan.get_next_mid();
            }
            fscan.close();
        }catch(Exception ex){
            System.err.println("Exception caused in creating BTree Index");
            ex.printStackTrace();
        }
    }

    public void deleteDuplicateRecords()
        throws IndexException,
            InvalidTypeException,
            InvalidTupleSizeException,
            UnknownIndexTypeException,
            UnknownKeyTypeException,
            java.io.IOException,
            InvalidSlotNumberException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            java.lang.Exception{

        iscan = new MapIndexScan(new IndexType(IndexType.B_Index), name, indexUtil, attrType, res_str_sizes, 4, 4, projlist, null, null, 1, true);
        Pair previousMapPair = iscan.get_next_mid();
        Pair curMapPair = iscan.get_next_mid();

        String prevKey = previousMapPair.getIndexKey();
        String curKey = "";

        List<Pair> duplicateMaps = new ArrayList<>();
        duplicateMaps.add(previousMapPair);
        MID mid;
        Map map;
        while(curMapPair!=null){
            curKey = curMapPair.getIndexKey();

            if(prevKey.equals(curKey)){
                duplicateMaps.add(curMapPair);
            }else{
                duplicateMaps = new ArrayList<>();
                duplicateMaps.add(curMapPair);
            }
            if(duplicateMaps.size() == 4){
                mid = duplicateMaps.get(0).getRid();
                _hf.deleteRecordMap(mid);
                duplicateMaps.remove(0);
            }
            prevKey = curKey;
            curMapPair = iscan.get_next_mid();
        }
        iscan.close();

        if(duplicateMaps.size() == 4){
            _hf.deleteRecordMap(duplicateMaps.get(0).getRid());
            duplicateMaps.remove(0);
        }
    }

    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {
        Stream stream = new Stream(this, orderType, rowFilter, columnFilter, valueFilter, numBuf);
        return stream;
    }

    public void insertIntoMainIndex(){
        try{
            FileScanMap fscan = new FileScanMap(getName(), null, null);
            Pair mapPair;
            mapPair = fscan.get_next_mid();
            while(mapPair!=null){
                insertIndex(mapPair.getRid(), mapPair.getMap());
                mapPair = fscan.get_next_mid();
            }
            fscan.close();
        }catch(Exception ex){
            System.err.println("Exception caused in creating BTree Index");
        }
    }


    public MID insertWithIndex(Map map)
        throws IOException,
            IndexException,
            InvalidSlotNumberException,
            UnknownKeyTypeException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            InvalidUpdateException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            InvalidTypeException,
            UnknownIndexTypeException,
            Exception
        {
            expr[0].operand2.string = map.getRowLabel();
            expr[1].operand2.string = map.getColumnLabel();
            int keyFldNum = 1;
            CondExpr[] condExprForKey = Stream.getKeyFilterForIndexType(6, map.getRowLabel(), map.getColumnLabel(), "*");

            iscan = new MapIndexScan(new IndexType(IndexType.B_Index), name, indexUtil, attrType, res_str_sizes, 4, 4, projlist, expr, condExprForKey, keyFldNum, false);
        ArrayList<Map> mapList = new ArrayList<Map>();
        HashMap<Map, MID> ridHashMap = new HashMap<Map, MID>();
        Pair t = iscan.get_next_mid();

        while (t != null) {
            Map temp = t.getMap();
            temp.setFldOffset(temp.getMapByteArray());
            if(temp.getRowLabel().equals(map.getRowLabel())&&temp.getColumnLabel().equals(map.getColumnLabel())){
                mapList.add(temp);
                ridHashMap.put(temp, t.getRid());
                if(mapList.size() == 3) {
                    break;
                }
            }
            t = iscan.get_next_mid();
        }
        iscan.close();
        Collections.sort(mapList, new Comparator<Map>() {
            @Override
            public int compare(Map a, Map b) {
                try {
                    return a.getTimeStamp()-b.getTimeStamp();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });

        if(mapList.size() < 3) {
            return _hf.insertRecordMap(map.getMapByteArray());
        } else {
            MID deleteMID = ridHashMap.get(mapList.get(0));
            _hf.deleteRecordMap(deleteMID);
//            removeIndex(deleteMID, mapList.get(0));
            removeIndexUtil(deleteMID, mapList.get(0));
            return _hf.insertRecordMap(map.getMapByteArray());
        }

    }

    public void unpinAllPages(){
        try{
            int num_pages  = SystemDefs.JavabaseBM.getNumBuffers();
            for(int i = 0; i <= num_pages; i++){
                PageId pageId = new PageId(i);
                try{
                    SystemDefs.JavabaseBM.unpinPage(pageId, true);
                }catch(Exception e){
//                    System.out.println("Page " + i + " Exception");
                    continue;
                }
            }
        }catch(Exception ex){
            System.err.println("Bigt.java unpinAllPages: Failed to unpin all pages");
            ex.printStackTrace();
        }

    }

    public void deleteIndexTree(BTreeFile bTreeFile)
    throws java.io.IOException,
            btree.IteratorException,
            btree.UnpinPageException,
            btree.FreePageException,
            btree.DeleteFileEntryException,
            btree.ConstructPageException,
            btree.PinPageException{
        bTreeFile.destroyFile();
    }
}
