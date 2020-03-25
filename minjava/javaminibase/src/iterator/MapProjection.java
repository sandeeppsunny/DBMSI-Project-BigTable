package iterator;

import BigT.Map;
import heap.*;
import global.*;

import java.io.*;

/**
 * JMap has the appropriate types.
 * JMap already has its setHdr called to setup its vital stas.
 */

public class MapProjection {
    /**
     * Map m1 and Map m2 will be joined, and the result
     * will be stored in Map Jmap,before calling this mehtod.
     * we know that this two map can join in the common field
     *
     * @param m1         The Map will be joined with m2
     * @param type1[]    The array used to store the each attribute type
     * @param m2         The Map will be joined with m1
     * @param type2[]    The array used to store the each attribute type
     * @param Jmap       the returned Map
     * @param perm_mat[] shows what input fields go where in the output tuple
     * @param nOutFlds   number of outer relation field
     * @throws UnknowAttrType                 attrbute type does't match
     * @throws FieldNumberOutOfBoundException field number exceeds limit
     * @throws IOException                    some I/O fault
     */
    public static void Join(Map m1, AttrType type1[],
                            Map m2, AttrType type2[],
                            Map Jmap, FldSpec perm_mat[],
                            int nOutFlds
    )
            throws UnknowAttrType,
            FieldNumberOutOfBoundException,
            IOException {


        for (int i = 0; i < nOutFlds; i++) {
            switch (perm_mat[i].relation.key) {
                case RelSpec.outer:        // Field of outer (t1)
                    switch (i) {
                        case 0:
                            Jmap.setRowLabel(m1.getRowLabel());
                            break;
                        case 1:
                            Jmap.setColumnLabel(m1.getColumnLabel());
                            break;
                        case 2:
                            Jmap.setTimeStamp(m1.getTimeStamp());
                            break;
                        case 3:
                            Jmap.setValue(m1.getValue());
                            break;
                        default:

                            throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");

                    }
                    break;

                case RelSpec.innerRel:        // Field of inner (t2)
                    switch (i) {
                        case 0:
                            Jmap.setRowLabel(m2.getRowLabel());
                            break;
                        case 1:
                            Jmap.setColumnLabel(m2.getColumnLabel());
                            break;
                        case 2:
                            Jmap.setTimeStamp(m2.getTimeStamp());
                            break;
                        case 3:
                            Jmap.setValue(m2.getValue());
                            break;
                        default:

                            throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");

                    }
                    break;
            }
        }
        return;
    }


    /**
     * Map m1 will be projected
     * the result will be stored in Map Jmap
     *
     * @param m1         The Map will be projected
     * @param type1[]    The array used to store the each attribute type
     * @param Jmap     the returned Map
     * @param perm_mat[] shows what input fields go where in the output tuple
     * @param nOutFlds   number of outer relation field
     * @throws UnknowAttrType                 attrbute type doesn't match
     * @throws WrongPermat                    wrong FldSpec argument
     * @throws FieldNumberOutOfBoundException field number exceeds limit
     * @throws IOException                    some I/O fault
     */

    public static void Project(Map m1, AttrType type1[],
                               Map Jmap, FldSpec perm_mat[],
                               int nOutFlds
    )
            throws UnknowAttrType,
            WrongPermat,
            FieldNumberOutOfBoundException,
            IOException {

        for (int i = 0; i < nOutFlds; i++) {
            switch (perm_mat[i].relation.key) {
                case RelSpec.outer:      // Field of outer (t1)
                    switch (i) {
                        case 0:
                            Jmap.setRowLabel(m1.getRowLabel());
                            break;
                        case 1:
                            Jmap.setColumnLabel(m1.getColumnLabel());
                            break;
                        case 2:
                            Jmap.setTimeStamp(m1.getTimeStamp());
                            break;
                        case 3:
                            Jmap.setValue(m1.getValue());
                            break;
                        default:

                            throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");

                    }
                    break;

                default:

                    throw new WrongPermat("something is wrong in perm_mat");

            }
        }
        return;
    }

}
