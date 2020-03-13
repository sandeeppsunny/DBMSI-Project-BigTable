package BigT;

import global.RID;

public class Pair {
    public RID getRid() {
        return rid;
    }

    public Map getMap() {
        return map;
    }

    Map map;
    RID rid;
    public Pair(Map map, RID rid) {
        this.map = map;
        this.rid = rid;
    }
}