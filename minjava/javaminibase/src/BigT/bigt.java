package BigT;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

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
    public ArrayList<Heapfile> heapFiles;
    public ArrayList<String> heapFileNames;
    public ArrayList<String> indexFileNames;
    public ArrayList<BTreeFile> indexFiles;
    public BTreeFile utilityIndex = null;
    public String indexUtil;
    private AttrType[] attrType;
    private FldSpec[] projlist;
    private CondExpr[] expr;
    MapIndexScan iscan;

    private int storageType;
    private int insertType;
    short[] res_str_sizes = new short[]{Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};


    public bigt(String name, boolean insert) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
            GetFileEntryException, ConstructPageException, AddFileEntryException, btree.IteratorException,
            btree.UnpinPageException, btree.FreePageException, btree.DeleteFileEntryException, btree.PinPageException,
            PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        String fileName = "";
        heapFiles = new ArrayList<>(6);
        indexFiles = new ArrayList<>(6);
        heapFileNames = new ArrayList<>(6);
        indexFileNames = new ArrayList<>(6);
        this.name = name;
        heapFiles.add(null);
        heapFileNames.add("");
        indexFileNames.add("");
        for(int i = 1; i <= 5; i++){
            heapFileNames.add(name + "_" + i);
            indexFileNames.add(name + "_index_" + i);
            heapFiles.add(new Heapfile(heapFileNames.get(i)));
        }

        indexUtil = name + "_" + "indexUtil";

        if(insert){
            //For multiple batch insert

            indexCreateUtil();
            createIndexUtil();

            try {
                deleteAllNodesInIndex(utilityIndex);
                for(int i=2; i<=5; i++) {
                    deleteAllNodesInIndex(indexFiles.get(i));
                }
            } catch(Exception e) {
                e.printStackTrace();
                System.out.println("Exception occurred while destroying all nodes of index files");
            }
//            if(utilityIndex != null){
//                utilityIndex.close();
//                createIndexUtil();
//                utilityIndex.destroyFile();
//            }
//
//            indexDestroyUtil();
//
//            createIndexUtil();
//            indexFiles = new ArrayList<>();
//            indexCreateUtil();
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

    public Heapfile getHeapFile(int i) {
        return heapFiles.get(i);
    }

    public int getType(){
        return this.storageType;
    }

    public String getIndexFileName(int i){
        return indexFileNames.get(i);
    }

    public String getHeapFileName(int i){
        return heapFileNames.get(i);
    }

    public String indexName(){
        return name + "_index_" + storageType;
    }

    public void indexCreateUtil() throws IOException,
            ConstructPageException,
            PinPageException,
            UnpinPageException,
            IteratorException,
            GetFileEntryException,
            DeleteFileEntryException,
            AddFileEntryException,
            FreePageException,
            PageUnpinnedException,
            InvalidFrameNumberException,
            HashEntryNotFoundException,
            ReplacerException {
        indexFiles.add(null);
        BTreeFile _index = null;
        for(int i = 1; i <= 5; i++){
            _index = createIndex(indexFileNames.get(i), i);
            indexFiles.add(_index);
            /*if (_index!=null) {
                _index.close();
            }*/
        }
    }

    public void indexDestroyUtil() throws DeleteFileEntryException,
            IteratorException,
            PinPageException,
            IOException,
            ConstructPageException,
            FreePageException,
            UnpinPageException, GetFileEntryException, AddFileEntryException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        BTreeFile tempIndex;
        for(int i=1; i<=5; i++){
            tempIndex = indexFiles.get(i);
            if(tempIndex!=null){
                tempIndex.close();
                tempIndex = createIndex(indexFileNames.get(i), i);
                tempIndex.destroyFile();
            }
        }
    }

    public void deleteAllNodesInIndex(BTreeFile index) throws PinPageException, KeyNotMatchException, IteratorException, IOException, ConstructPageException, UnpinPageException, ScanIteratorException, ScanDeleteException {
        BTFileScan scan = index.new_scan(null, null);
        boolean isScanComplete = false;
        while(!isScanComplete) {
            KeyDataEntry entry = scan.get_next();
            if(entry == null) {
                isScanComplete = true;
                break;
            }
            scan.delete_current();
        }
    }

    public BTreeFile createIndex(String indexName1, int type) throws GetFileEntryException,
            ConstructPageException,
            IOException,
            AddFileEntryException,
            btree.IteratorException,
            btree.UnpinPageException,
            btree.FreePageException,
            btree.DeleteFileEntryException,
            btree.PinPageException {
        BTreeFile tempIndex=null;
        switch(type){
            case 1:
                break;
            case 2:
                tempIndex = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                break;
            case 3:
                tempIndex = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_STRING_ATTRIBUTE_SIZE, DeleteFashion.NAIVE_DELETE);
                break;
            case 4:
                tempIndex = new BTreeFile(indexName1, AttrType.attrString,
                        Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5, DeleteFashion.NAIVE_DELETE);
                break;
            case 5:
                tempIndex = new BTreeFile(indexName1, AttrType.attrString,
                        Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5, DeleteFashion.NAIVE_DELETE);
                break;
        }
        return tempIndex;
    }

    public void createIndexUtil(){
        try {
            utilityIndex = new BTreeFile(indexUtil, AttrType.attrString,
                    Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 20, DeleteFashion.NAIVE_DELETE);
        }catch(Exception ex){
            System.err.println("Error in creating utility index");
            ex.printStackTrace();
        }
    }

    public void insertIndex(MID mid, Map map, int type) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
            IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
            NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException, IOException {
        switch (type) {
            case 1:
                break;
            case 2:
                indexFiles.get(2).insert(new StringKey(map.getRowLabel()), mid);
                break;
            case 3:
                indexFiles.get(3).insert(new StringKey(map.getColumnLabel()), mid);
                break;
            case 4:
                indexFiles.get(4).insert(new StringKey(map.getColumnLabel() + "%" + map.getRowLabel()), mid);
                break;
            case 5:
                indexFiles.get(5).insert(new StringKey(map.getRowLabel() + "%" + map.getValue()), mid);
                break;
        }
    }

    public boolean removeIndex(MID mid, Map map, int type)
            throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
            ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
            DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
            IOException, DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
            FreePageException, RecordNotFoundException, IndexFullDeleteException {
        boolean status;
        if(type == 1){
            status = true;
        }else if(type == 2){
            status = indexFiles.get(2).Delete(new StringKey(map.getRowLabel()), mid);
        }else if(type == 3){
            status = indexFiles.get(3).Delete(new StringKey(map.getColumnLabel()), mid);
        }else if (type == 4){
            status = indexFiles.get(4).Delete(new StringKey(map.getColumnLabel() + "%" + map.getRowLabel()), mid);
        }else{
            status = indexFiles.get(5).Delete(new StringKey(map.getRowLabel() + "%" + map.getValue()), mid);
        }
        return status;
    }

    public void insertIndexUtil(MID mid, Map map, int heapFileIndex)
            throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
            ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
            DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
            IOException{
//        System.out.println("Key length " + (map.getRowLabel() + map.getColumnLabel()).length());
//        System.out.println("Index key is " + (new StringKey(map.getRowLabel() + map.getColumnLabel())));
        utilityIndex.insert(new StringKey(map.getRowLabel() + map.getColumnLabel() + "%" + map.getTimeStamp() + "%" + heapFileIndex), mid);
    }

    public void removeIndexUtil(MID mid, Map map, int heapFileIndex)
            throws KeyNotMatchException, IndexInsertRecException,
            ConstructPageException, UnpinPageException, PinPageException,
            DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException,
            IOException, DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
            FreePageException, RecordNotFoundException, IndexFullDeleteException{
        utilityIndex.Delete(new StringKey(map.getColumnLabel() + map.getRowLabel() + "%" + map.getTimeStamp() + "%" + heapFileIndex), mid);
    }

    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        int totalMapCount = 0;
        for(int i = 1; i <= 5; i++){
            totalMapCount += heapFiles.get(i).getRecCntMap();
        }
        return totalMapCount;
    }

    public int getRowCnt()  throws Exception{
        return getCount(3);
    }

    public int getColumnCnt()  throws Exception{
        return getCount(4);
    }

    public int getCount(int orderType) throws Exception{
        int numBuf = (int)((SystemDefs.JavabaseBM.getNumBuffers()*3)/4);
//        CombinedStream stream = new CombinedStream(this, orderType,"*","*","*",numBuf);
        Stream stream = new Stream(this.name, null, 1,  orderType, "*", "*", "*", numBuf);
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


    public MID insertMap(Map map, int type) throws HFDiskMgrException,
            InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException,
            InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException {
        this.insertType = type;
        MID mid = heapFiles.get(type).insertRecordMap(map.getMapByteArray());
        return mid;
    }

    public MID insertMapIntoAlreadySortedFile(Map map, int type) throws HFDiskMgrException,
            InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException,
            InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException {
        this.insertType = type;
        MID mid = heapFiles.get(type).insertRecordMapIntoSortedFile(map.getMapByteArray(), type);
        return mid;
    }

    public void buildUtilityIndex(){
        try{
//            System.out.println("Building Utility INdex");

            /*
            For phase3, the key for the utility index will be
            (row+columnLabel % timestamp % i)
            Where i corresponds to the i-th heap file
            */
            FileScanMap fscan;
            String heapFileName;
            for(int i = 1; i<= 5; i++){
                heapFileName = getHeapFileName(i);
                fscan = new FileScanMap(heapFileName, null, null, false);
                Pair mapPair;
                mapPair = fscan.get_next_mid();
                while(mapPair!=null){
//                mapPair.getMap().print();
                    insertIndexUtil(mapPair.getMid(), mapPair.getMap(), i);
                    mapPair = fscan.get_next_mid();
                }
                fscan.close();
            }
        }catch(Exception ex){
            System.err.println("Exception caused in creating BTree Index");
            ex.printStackTrace();
        }
    }


    public void sortHeapFiles() {
        String tempFileName;
        try{
            if(this.insertType != 1){
                FileScanMap fscan;
                String heapFileName;
                heapFileName = heapFileNames.get(this.insertType);
                fscan = new FileScanMap(heapFileName, null, null, false);
                int sortType = 1;
                switch (this.insertType) {
                    case 3:
                    case 4:
                        sortType = 2;
                        break;
                    case 5:
                        sortType = 6;
                        break;
                    default:
                        sortType = 1;
                        break;
                }
//                System.out.println("Number of unpinned Buffers " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
//                System.out.println("Number of buffers " + SystemDefs.JavabaseBM.getNumBuffers());
//                SystemDefs.JavabaseBM.flushAllPagesForcibly();
                SortMap sortMap = new SortMap(null, null, null,
                        fscan, sortType, new MapOrder(MapOrder.Ascending), null,
                        (int)((SystemDefs.JavabaseBM.getNumBuffers()*3)/4));

                Heapfile fileToDestroy = new Heapfile(null);
                tempFileName = fileToDestroy._fileName;
                boolean isScanComplete = false;
                MID resultMID = new MID();
                while (!isScanComplete) {
                    Map map = sortMap.get_next();
                    if (map == null) {
                        isScanComplete = true;
                        break;
                    }
                    map.setFldOffset(map.getMapByteArray());
                    fileToDestroy.insertRecordMap(map.getMapByteArray());
                }
                sortMap.close();

                getHeapFile(this.insertType).deleteFileMap();
                heapFiles.set(this.insertType, new Heapfile(heapFileName));

                fscan = new FileScanMap(tempFileName, null, null, false);
                isScanComplete = false;
                resultMID = new MID();
                while (!isScanComplete) {
                    Map map = fscan.get_next();
                    if (map == null) {
                        isScanComplete = true;
                        break;
                    }
                    map.setFldOffset(map.getMapByteArray());
                    getHeapFile(this.insertType).insertRecordMap(map.getMapByteArray());
                }
                fscan.close();
                fileToDestroy.deleteFileMap();
            }
        } catch(Exception ex) {
            System.err.println("Exception caused while creating sorted heapfiles.");
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
            HFDiskMgrException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {

        iscan = new MapIndexScan(new IndexType(IndexType.B_Index), this.getHeapFileName(1), indexUtil, attrType, res_str_sizes, 4, 4, projlist, null, null, 1, true);
        Pair previousMapPair = iscan.get_next_mid();
        Pair curMapPair = iscan.get_next_mid();

        String[] indexKeyTokens;

        String prevKey = previousMapPair.getIndexKey();
//        System.out.println("Index Key is: " + prevKey);
        String curKey = "";

        List<Pair> duplicateMaps = new ArrayList<>();
        indexKeyTokens = prevKey.split("%");
        previousMapPair  = new Pair(previousMapPair.getMap(), previousMapPair.getMid(), previousMapPair.getIndexKey(),
                Integer.parseInt(indexKeyTokens[indexKeyTokens.length-1]));
        duplicateMaps.add(previousMapPair);
        MID mid;
        Map map;
        while(curMapPair!=null){
            curKey = curMapPair.getIndexKey();
//            System.out.println("Index Key is: " + curKey);
            indexKeyTokens = curKey.split("%");
            String curKeyString = curKey.substring(0, curKey.indexOf('%'));
            String prevKeyString = prevKey.substring(0, prevKey.indexOf('%'));
//            System.out.println("Previous Key: " + prevKeyString);
//            System.out.println("Current Key: " + curKeyString);
            curMapPair = new Pair(curMapPair.getMap(), curMapPair.getMid(), curMapPair.getIndexKey(),
                    Integer.parseInt(indexKeyTokens[indexKeyTokens.length-1]));

            if(prevKeyString.equals(curKeyString)){
                duplicateMaps.add(curMapPair);
            }else{
                duplicateMaps = new ArrayList<>();
                duplicateMaps.add(curMapPair);
            }
            if(duplicateMaps.size() == 4){
                duplicateMaps.sort(new Comparator<Pair>() {
                    @Override
                    public int compare(Pair o1, Pair o2) {
                        String o1String = o1.getIndexKey();
                        String o2String = o2.getIndexKey();

                        Integer o1Timestamp = Integer.parseInt(o1.getIndexKey().split("%")[1]);
                        Integer o2Timestamp = Integer.parseInt(o2.getIndexKey().split("%")[1]);
                        return o1Timestamp.compareTo(o2Timestamp);
                    }
                });
                mid = duplicateMaps.get(0).getMid();
//                System.out.println("Heap File index: " + duplicateMaps.get(0).getHeapFileIndex());
                heapFiles.get(duplicateMaps.get(0).getHeapFileIndex()).deleteRecordMap(mid);
//                _hf.deleteRecordMap(mid);
                duplicateMaps.remove(0);
            }
            prevKey = curKey;
            curMapPair = iscan.get_next_mid();
        }
        iscan.close();
        utilityIndex.close();

        if(duplicateMaps.size() == 4){
            mid = duplicateMaps.get(0).getMid();
            heapFiles.get(duplicateMaps.get(0).getHeapFileIndex()).deleteRecordMap(mid);
//            _hf.deleteRecordMap(duplicateMaps.get(0).getMid());
            duplicateMaps.remove(0);
        }
    }

    public void duplicateRecordsMapInsert(String bigtName, Map insertMap) throws IOException,
            FileScanException, TupleUtilsException, InvalidRelation, JoinsException, FieldNumberOutOfBoundException,
            PageNotReadException, WrongPermat, InvalidTypeException, InvalidTupleSizeException, PredEvalException,
            UnknowAttrType, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException,
            RedistributeException, ConvertException, IteratorException, DeleteRecException, NodeNotMatchException,
            IndexSearchException, RecordNotFoundException, ConstructPageException, UnpinPageException,
            InsertRecException, KeyNotMatchException, IndexInsertRecException, PinPageException, InsertException,
            DeleteFashionException, KeyTooLongException, LeafDeleteException, FreePageException,
            IndexFullDeleteException, LeafInsertRecException, LeafRedistributeException {
            CondExpr[] condExprs = new CondExpr[3];
            CondExpr expr1 = new CondExpr();
            expr1.op = new AttrOperator(AttrOperator.aopEQ);
            expr1.type1 = new AttrType(AttrType.attrSymbol);
            expr1.type2 = new AttrType(AttrType.attrString);
            expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr1.operand2.string = insertMap.getRowLabel();
            expr1.next = null;
            condExprs[0] = expr1;

            CondExpr expr2 = new CondExpr();
            expr2.op = new AttrOperator(AttrOperator.aopEQ);
            expr2.type1 = new AttrType(AttrType.attrSymbol);
            expr2.type2 = new AttrType(AttrType.attrString);
            expr2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr2.operand2.string = insertMap.getColumnLabel();
            expr2.next = null;
            condExprs[1] = expr2;
            condExprs[2] = null;

            FileScanMap tempScan = new FileScanMap(bigtName, null, condExprs, true);
            MID tempMID = new MID();
            int timeStamp = Integer.MAX_VALUE;

            int count = 0;

            Pair tempPair = null;
            tempPair = tempScan.get_next_mid();
            Map tempMap = null;

            while(tempPair!=null){
                count += 1;
                if(timeStamp >= tempPair.getMap().getTimeStamp()){
                    tempMap = new Map(tempPair.getMap());
                    tempMID = tempPair.getMid();
                    timeStamp = tempMap.getTimeStamp();
                }

                tempPair = tempScan.get_next_mid();
            }
            boolean status;
            if(count > 3){
                for(int i = 1; i < heapFiles.size(); i++){
                    if(heapFiles.get(i).deleteRecordMap(tempMID)){
                        status = removeIndex(tempMID, tempMap, i);
                        System.out.println("Index entry removal for the duplicate map: STATUS: " + status);
                        break;
                    }
                }
            }

    }

    public Stream openStream(String bigTableName, int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {
        Stream stream = new Stream(bigTableName, null, 2, orderType, rowFilter, columnFilter, valueFilter, numBuf);
        return stream;
    }

    public void insertIntoMainIndex(){
        FileScanMap fscan;
        for(int i = 2; i <= 5; i++){
            try{
                indexFiles.set(i, createIndex(indexFileNames.get(i), i));
                fscan = new FileScanMap(heapFileNames.get(i), null, null, false);
                Pair mapPair;
                mapPair = fscan.get_next_mid();
                while(mapPair!=null){
                    insertIndex(mapPair.getMid(), mapPair.getMap(), i);
                    mapPair = fscan.get_next_mid();
                }
                fscan.close();
                indexFiles.get(i).close();
            }catch(Exception ex){
                System.err.println("Exception caused in creating BTree Index for storage index type: " + i);
                ex.printStackTrace();
            }

        }


    }

    public void createMapInsertIndex(int type){
        if(type == 1) {
            return;
        }
        FileScanMap fscan;
        try{
            deleteAllNodesInIndex(indexFiles.get(type));
        }catch(Exception e){
            System.err.println("Exception caused in deleting records in BTree index for storage type: " + type);
        }
        try{
            indexFiles.set(type, createIndex(indexFileNames.get(type), type));
            fscan = new FileScanMap(heapFileNames.get(type), null, null, false);
            Pair mapPair;
            mapPair = fscan.get_next_mid();
            while(mapPair!=null){
                insertIndex(mapPair.getMid(), mapPair.getMap(), type);
                mapPair = fscan.get_next_mid();
            }
            fscan.close();
            indexFiles.get(type).close();
        }catch(Exception ex){
            System.err.println("Exception caused in creating BTree Index for storage index type: " + type);
            ex.printStackTrace();
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

    public void deleteRecordMap(MID mid, int type) throws HFDiskMgrException, InvalidTupleSizeException,
            HFException, IOException, InvalidSlotNumberException, HFBufMgrException {
        getHeapFile(type).deleteRecordMap(mid);
    }
}
