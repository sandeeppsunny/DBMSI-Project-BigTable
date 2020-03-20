package BigT;

import global.MID;

public class Pair {
    Map map;
    MID mid;
    String indexKey;

    public MID getRid() {
        return mid;
    }

    public Map getMap() {
        return map;
    }

    public String getIndexKey(){
        return indexKey;
    }


    public Pair(Map map, MID mid) {
        this.map = map;
        this.mid = mid;
    }
    public Pair(Map map, MID mid, String indexKey){
        this.map = map;
        this.mid = mid;
        this.indexKey = indexKey;
    }
}