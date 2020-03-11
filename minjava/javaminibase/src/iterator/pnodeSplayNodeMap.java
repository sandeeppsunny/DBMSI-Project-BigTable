
package iterator;

/**
 * An element in the binary tree.
 * including pointers to the children, the parent in addition to the item.
 */
public class pnodeSplayNodeMap {
    /**
     * a reference to the element in the node
     */
    public pnodeMap item;

    /**
     * the left child pointer
     */
    public pnodeSplayNodeMap lt;

    /**
     * the right child pointer
     */
    public pnodeSplayNodeMap rt;

    /**
     * the parent pointer
     */
    public pnodeSplayNodeMap par;

    /**
     * class constructor, sets all pointers to <code>null</code>.
     *
     * @param h the element in this node
     */
    public pnodeSplayNodeMap(pnodeMap h) {
        item = h;
        lt = null;
        rt = null;
        par = null;
    }

    /**
     * class constructor, sets all pointers.
     *
     * @param h the element in this node
     * @param l left child pointer
     * @param r right child pointer
     */
    public pnodeSplayNodeMap(pnodeMap h, pnodeSplayNodeMap l, pnodeSplayNodeMap r) {
        item = h;
        lt = l;
        rt = r;
        par = null;
    }

    /**
     * a static dummy node for use in some methods
     */
    public static pnodeSplayNodeMap dummy = new pnodeSplayNodeMap(null);

}

