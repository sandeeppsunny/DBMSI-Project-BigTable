package global;

/**
 * MID is the map id which consists of a pageid and slotid to identify a specific map entry.
 */
public class MID {

    /**
     * SlotNo to represent the slot number
     */
    public int slotNo;

    /**
     * PageID pageId object has the pid used to create the mapid
     */
    public PageId pageNo = new PageId();

    public MID() {

    }

    public MID(PageId pageNo, int slotNo) {
        this.pageNo = pageNo;
        this.slotNo = slotNo;
    }

    public void copyMID(MID fromMID) {
        this.pageNo = fromMID.pageNo;
        this.slotNo = fromMID.slotNo;
    }

    /**
     * Write MID to the byte array
     *
     * @param ary
     * @param offset
     * @throws java.io.IOException
     */
    public void writeToByteArray(byte[] ary, int offset) throws java.io.IOException {
        Convert.setIntValue(slotNo, offset, ary);
        Convert.setIntValue(pageNo.pid, offset + 4, ary);
    }

    /**
     * Compares two MID object, i.e, this to the mid
     *
     * @param mid MID object to be compared to
     * @return true is they are equal false if not.
     */
    public boolean equals(MID mid) {

        if ((this.pageNo.pid == mid.pageNo.pid) && (this.slotNo == mid.slotNo))
            return true;
        else
            return false;
    }
}
