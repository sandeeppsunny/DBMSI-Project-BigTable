package btree;

import global.*;

/**
 * IndexData: It extends the DataClass.
 * It defines the data "mid" for leaf node in B++ tree.
 */
public class LeafData extends DataClass {
    private MID myRid;

    public String toString() {
        String s;
        s = "[ " + (new Integer(myRid.pageNo.pid)).toString() + " "
                + (new Integer(myRid.slotNo)).toString() + " ]";
        return s;
    }

    /**
     * Class constructor
     *
     * @param mid the data mid
     */
    LeafData(MID mid) {
        myRid = new MID(mid.pageNo, mid.slotNo);
    }

    ;

    /**
     * get a copy of the mid
     *
     * @return the reference of the copy
     */
    public MID getData() {
        return new MID(myRid.pageNo, myRid.slotNo);
    }

    ;

    /**
     * set the mid
     */
    public void setData(MID mid) {
        myRid = new MID(mid.pageNo, mid.slotNo);
    }

    ;
}   
