package BigT;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

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
    // PageId _firstDirPageId; // page number of header page
    // int _ftype;
    // private boolean _file_deleted;
    // private String _fileName;
    // private static int tempfilecount = 0;

    private Heapfile _hf;
    private BTreeFile index1 = null;
    private BTreeFile index2 = null;
    public String indexName1;
    public String indexName2;
    private int type;
    AttrType[] attrType;
    FldSpec[] projlist;
    CondExpr[] expr;
    MapIndexScan iscan;
    short[] res_str_sizes = new short[]{Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};


    public bigt(java.lang.String name, int type) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
            GetFileEntryException, ConstructPageException, AddFileEntryException {
        String file_name = name+type;
        _hf = new Heapfile(file_name);
        this.name = file_name;
        this.type = type;
        indexName1 = "index1";
        indexName2 = "index2";
        createIndex(indexName1, indexName2);
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
            AddFileEntryException {
        switch(type){
            case 1:
                break;
            case 2:
                index1 = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                break;
            case 3:
                index1 = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                break;
            case 4:
                index1 = new BTreeFile(indexName1, AttrType.attrString, 3*Map.DEFAULT_STRING_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                index2 = new BTreeFile(indexName2, AttrType.attrInteger, 4, DeleteFashion.FULL_DELETE);
                break;
            case 5:
                index1 = new BTreeFile(indexName1, AttrType.attrString, 3*Map.DEFAULT_STRING_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                index2 = new BTreeFile(indexName2, AttrType.attrInteger, 4, DeleteFashion.FULL_DELETE);
                break;
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

    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        return _hf.getRecCntMap();
    }

    public int getRowCnt()  throws Exception{
        if(type==1){
            return getCount(true);
        }else{
            return getCountWithIndex(true);
        }
    }

    public int getColumnCnt()  throws Exception{
        if(type==1){
            return getCount(false);
        }else{
            return getCountWithIndex(false);
        }
    }

    public int getCount(boolean countType) throws Exception{
        Scan scan = _hf.openScanMap();
        MID mid = new MID();
        Map map = new Map();
        map = scan.getNextMap(mid);
        HashSet<String> distinct = new HashSet<String>();
        while(map!=null){
            map.setFldOffset(map.getMapByteArray());
            if(countType){
                distinct.add(map.getRowLabel());
            }else{
                distinct.add(map.getColumnLabel());
            }
            map = scan.getNextMap(mid);
        }
        return distinct.size();
    }

    public int getCountWithIndex(boolean countType)  throws Exception{
        MapIndexScan mapIndexScan = new MapIndexScan(new IndexType(IndexType.B_Index), name, indexName1, attrType, res_str_sizes, 4, 4, projlist, null,null, 1, false);
        Pair pair = mapIndexScan.get_next_mid();
        HashSet<String> distinct = new HashSet<String>();
        while(pair!=null){
            Map map = pair.getMap();
            map.setFldOffset(map.getMapByteArray());
            if(countType){
                distinct.add(map.getRowLabel());
            }else{
                distinct.add(map.getColumnLabel());
            }
            pair = mapIndexScan.get_next_mid();
        }
        return distinct.size();
    }

    public MID insertMap(Map map) throws DeleteFashionException, LeafRedistributeException, RedistributeException,
            InsertRecException, FreePageException, RecordNotFoundException, IndexFullDeleteException, Exception {

        if(type==1){
            MID mid = _hf.insertRecordMapWithoutIndex(map.getMapByteArray());
            insertIndex(mid, map);
            return mid;
        }
        else{
            map.setFldOffset(map.getMapByteArray());
            MID mid = insertWithIndex(map);
            insertIndex(mid, map);
//            MID mid = _hf.insertRecordMap(map.getMapByteArray());
//            insertIndex(mid, map);
            return mid;
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
            CondExpr[] condExprForKey = Stream.getKeyFilterForIndexType(type, map.getRowLabel(), map.getColumnLabel(), "*");
            int keyFldNum = 1;
            switch(type) {
                case 2:
                    keyFldNum = 1;
                    break;
                case 3:
                    keyFldNum = 2;
                    break;
                case 4:
                    keyFldNum = 2;
                    break;
                case 5:
                    keyFldNum = 1;
                    break;
            }
            iscan = new MapIndexScan(new IndexType(IndexType.B_Index), name, indexName1, attrType, res_str_sizes, 4, 4, projlist, expr, condExprForKey, keyFldNum, false);

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
        // System.out.println("-----------------------");
        // for(int i=0; i<mapList.size(); i++) {
        //     mapList.get(i).setFldOffset(mapList.get(i).getMapByteArray());
        //     mapList.get(i).print();
        // }
        // System.out.println(getMapCnt()+" "+mapList.size());
        // System.out.println("-----------------------");

        if(mapList.size() < 3) {
            return _hf.insertRecordMap(map.getMapByteArray());
        } else {
            MID deleteMID = ridHashMap.get(mapList.get(0));
            _hf.deleteRecordMap(deleteMID);
            removeIndex(deleteMID, mapList.get(0));

            return _hf.insertRecordMap(map.getMapByteArray());
        }

    }

    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {
        Stream stream = new Stream(this, orderType, rowFilter, columnFilter, valueFilter, numBuf);
        return stream;
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
}
