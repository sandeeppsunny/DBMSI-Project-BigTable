package BigT;

import global.MID;

public class Pair {
    Map map;
    MID mid;
    String indexKey;
    int heapFileIndex;

    public MID getMid() {
        return mid;
    }

    public Map getMap() {
        return map;
    }

    public String getIndexKey(){
        return indexKey;
    }

    public int getHeapFileIndex() { return heapFileIndex; }


    public Pair(Map map, MID mid) {
        this.map = map;
        this.mid = mid;
    }
    public Pair(Map map, MID mid, String indexKey){
        this.map = map;
        this.mid = mid;
        this.indexKey = indexKey;
    }
    public Pair(Map map, MID mid, String indexKey, int heapFileIndex){
        this.map = map;
        this.mid = mid;
        this.indexKey = indexKey;
        this.heapFileIndex = heapFileIndex;
    }
}