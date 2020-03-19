package BigT;

import global.MID;

public class Pair {
    public MID getRid() {
        return mid;
    }

    public Map getMap() {
        return map;
    }

    Map map;
    MID mid;
    public Pair(Map map, MID mid) {
        this.map = map;
        this.mid = mid;
    }
}