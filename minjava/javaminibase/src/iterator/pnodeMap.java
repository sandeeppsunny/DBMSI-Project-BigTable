package iterator;

import BigT.Map;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;

/**
 * A structure describing a map.
 * include a run number and the map
 */
public class pnodeMap {
    /**
     * which run does this tuple belong
     */
    public int run_num;

    /**
     * the tuple reference
     */
    public Map map;

    /**
     * class constructor, sets <code>run_num</code> to 0 and <code>map</code>
     * to null.
     */
    public pnodeMap() {
        run_num = 0;  // this may need to be changed
        map = null;
    }

    /**
     * class constructor, sets <code>run_num</code> and <code>map</code>.
     *
     * @param runNum the run number
     * @param t      the map
     */
    public pnodeMap(int runNum, Map t) {
        run_num = runNum;
        map = t;
    }

}

