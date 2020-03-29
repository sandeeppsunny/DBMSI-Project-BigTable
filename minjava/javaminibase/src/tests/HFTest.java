package tests;

import java.io.*;
import java.util.*;
import java.lang.*;

import BigT.Map;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import chainexception.*;
import iterator.MapUtils;

import static tests.MapUnitTest.*;

/**
 * Note that in JAVA, methods can't be overridden to be more private.
 * Therefore, the declaration of all private functions are now declared
 * protected as opposed to the private type in C++.
 */

class HFDriver extends TestDriver implements GlobalConst {

    private final static boolean OK = true;
    private final static boolean FAIL = false;

    private int choice;
    private final static int reclen = 32;

    public HFDriver() {
        super("hptest");
        choice = 100;      // big enough for file to occupy > 1 data page
        //choice = 2000;   // big enough for file to occupy > 1 directory page
        //choice = 5;
    }


    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 150, 100, "Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;
    }

    protected boolean test1() {

        System.out.println("\n  Test 1: Insert and scan fixed-size records\n");
        boolean status = OK;
        MID mid = new MID();
        Heapfile f = null;

        System.out.println("  - Create a heap file\n");
        try {
            f = new Heapfile("file_1");
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap file has left pages pinned\n");
            status = FAIL;
        }

        if (status == OK) {
            System.out.println("  - Add " + choice + " records to the file\n");
            for (int i = 0; (i < choice) && (status == OK); i++) {

                //fixed length record
                DummyRecord rec = new DummyRecord(reclen);
                rec.ival = i;
                rec.fval = (float) (i * 2.5);
                rec.name = "record" + i;

                try {
                    mid = f.insertRecordTuple(rec.toByteArray());
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Error inserting record " + i + "\n");
                    e.printStackTrace();
                }

                if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                        != SystemDefs.JavabaseBM.getNumBuffers()) {

                    System.err.println("*** Insertion left a page pinned\n");
                    status = FAIL;
                }
            }

            try {
                if (f.getRecCntTuple() != choice) {
                    status = FAIL;
                    System.err.println("*** File reports " + f.getRecCntTuple() +
                            " records, not " + choice + "\n");
                }
            } catch (Exception e) {
                status = FAIL;
                System.out.println("" + e);
                e.printStackTrace();
            }
        }

        // In general, a sequential scan won't be in the same order as the
        // insertions.  However, we're inserting fixed-length records here, and
        // in this case the scan must return the insertion order.

        Scan scan = null;

        if (status == OK) {
            System.out.println("  - Scan the records just inserted\n");

            try {
                scan = f.openScanTuple();
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }

            if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    == SystemDefs.JavabaseBM.getNumBuffers()) {
                System.err.println("*** The heap-file scan has not pinned the first page\n");
                status = FAIL;
            }
        }

        if (status == OK) {
            int len, i = 0;
            DummyRecord rec = null;
            Tuple tuple = new Tuple();

            boolean done = false;
            while (!done) {
                try {
                    tuple = scan.getNextTuple(mid);
                    if (tuple == null) {
                        done = true;
                        break;
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                if (status == OK && !done) {
                    try {
                        rec = new DummyRecord(tuple);
                    } catch (Exception e) {
                        System.err.println("" + e);
                        e.printStackTrace();
                    }

                    len = tuple.getLength();
                    if (len != reclen) {
                        System.err.println("*** Record " + i + " had unexpected length "
                                + len + "\n");
                        status = FAIL;
                        break;
                    } else if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                            == SystemDefs.JavabaseBM.getNumBuffers()) {
                        System.err.println("On record " + i + ":\n");
                        System.err.println("*** The heap-file scan has not left its " +
                                "page pinned\n");
                        status = FAIL;
                        break;
                    }
                    String name = ("record" + i);

                    if ((rec.ival != i)
                            || (rec.fval != (float) i * 2.5)
                            || (!name.equals(rec.name))) {
                        System.err.println("*** Record " + i
                                + " differs from what we inserted\n");
                        System.err.println("rec.ival: " + rec.ival
                                + " should be " + i + "\n");
                        System.err.println("rec.fval: " + rec.fval
                                + " should be " + (i * 2.5) + "\n");
                        System.err.println("rec.name: " + rec.name
                                + " should be " + name + "\n");
                        status = FAIL;
                        break;
                    }
                }
                ++i;
            }

            //If it gets here, then the scan should be completed
            if (status == OK) {
                if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                        != SystemDefs.JavabaseBM.getNumBuffers()) {
                    System.err.println("*** The heap-file scan has not unpinned " +
                            "its page after finishing\n");
                    status = FAIL;
                } else if (i != (choice)) {
                    status = FAIL;

                    System.err.println("*** Scanned " + i + " records instead of "
                            + choice + "\n");
                }
            }
        }

        if (status == OK)
            System.out.println("  Test 1 completed successfully.\n");

        return status;
    }

    protected boolean test2() {

        System.out.println("\n  Test 2: Delete fixed-size records\n");
        boolean status = OK;
        Scan scan = null;
        MID mid = new MID();
        Heapfile f = null;

        System.out.println("  - Open the same heap file as test 1\n");
        try {
            f = new Heapfile("file_1");
        } catch (Exception e) {
            status = FAIL;
            System.err.println(" Could not open heapfile");
            e.printStackTrace();
        }

        if (status == OK) {
            System.out.println("  - Delete half the records\n");
            try {
                scan = f.openScanTuple();
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }
        }

        if (status == OK) {
            int len, i = 0;
            Tuple tuple = new Tuple();
            boolean done = false;

            while (!done) {
                try {
                    tuple = scan.getNextTuple(mid);
                    if (tuple == null) {
                        done = true;
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                if (!done && status == OK) {
                    boolean odd = true;
                    if (i % 2 == 1) odd = true;
                    if (i % 2 == 0) odd = false;
                    if (odd) {       // Delete the odd-numbered ones.
                        try {
                            status = f.deleteRecordTuple(mid);
                        } catch (Exception e) {
                            status = FAIL;
                            System.err.println("*** Error deleting record " + i + "\n");
                            e.printStackTrace();
                            break;
                        }
                    }
                }
                ++i;
            }
        }

        scan.closescan();    //  destruct scan!!!!!!!!!!!!!!!
        scan = null;

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {

            System.out.println("\nt2: in if: Number of unpinned buffers: "
                    + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "\n");
            System.err.println("t2: in if: getNumbfrs: " + SystemDefs.JavabaseBM.getNumBuffers() + "\n");

            System.err.println("*** Deletion left a page pinned\n");
            status = FAIL;
        }

        if (status == OK) {
            System.out.println("  - Scan the remaining records\n");
            try {
                scan = f.openScanTuple();
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }
        }

        if (status == OK) {
            int len, i = 0;
            DummyRecord rec = null;
            Tuple tuple = new Tuple();
            boolean done = false;

            while (!done) {
                try {
                    tuple = scan.getNextTuple(mid);
                    if (tuple == null) {
                        done = true;
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                if (!done && status == OK) {
                    try {
                        rec = new DummyRecord(tuple);
                    } catch (Exception e) {
                        System.err.println("" + e);
                        e.printStackTrace();
                    }

                    if ((rec.ival != i) ||
                            (rec.fval != (float) i * 2.5)) {
                        System.err.println("*** Record " + i
                                + " differs from what we inserted\n");
                        System.err.println("rec.ival: " + rec.ival
                                + " should be " + i + "\n");
                        System.err.println("rec.fval: " + rec.fval
                                + " should be " + (i * 2.5) + "\n");
                        status = FAIL;
                        break;
                    }
                    i += 2;     // Because we deleted the odd ones...
                }
            }
        }

        if (status == OK)
            System.out.println("  Test 2 completed successfully.\n");
        return status;

    }

    protected boolean test3() {

        System.out.println("\n  Test 3: Update fixed-size records\n");
        boolean status = OK;
        Scan scan = null;
        MID mid = new MID();
        Heapfile f = null;

        System.out.println("  - Open the same heap file as tests 1 and 2\n");
        try {
            f = new Heapfile("file_1");
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if (status == OK) {
            System.out.println("  - Change the records\n");
            try {
                scan = f.openScanTuple();
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }
        }

        if (status == OK) {

            int len, i = 0;
            DummyRecord rec = null;
            Tuple tuple = new Tuple();
            boolean done = false;

            while (!done) {
                try {
                    tuple = scan.getNextTuple(mid);
                    if (tuple == null) {
                        done = true;
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                if (!done && status == OK) {
                    try {
                        rec = new DummyRecord(tuple);
                    } catch (Exception e) {
                        System.err.println("" + e);
                        e.printStackTrace();
                    }

                    rec.fval = (float) 7 * i;     // We'll check that i==rec.ival below.

                    Tuple newTuple = null;
                    try {
                        newTuple = new Tuple(rec.toByteArray(), 0, rec.getRecLength());
                    } catch (Exception e) {
                        status = FAIL;
                        System.err.println("" + e);
                        e.printStackTrace();
                    }
                    try {
                        status = f.updateRecordTuple(mid, newTuple);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    if (status != OK) {
                        System.err.println("*** Error updating record " + i + "\n");
                        break;
                    }
                    i += 2;     // Recall, we deleted every other record above.
                }
            }
        }

        scan = null;

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {


            System.out.println("t3, Number of unpinned buffers: "
                    + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "\n");
            System.err.println("t3, getNumbfrs: " + SystemDefs.JavabaseBM.getNumBuffers() + "\n");

            System.err.println("*** Updating left pages pinned\n");
            status = FAIL;
        }

        if (status == OK) {
            System.out.println("  - Check that the updates are really there\n");
            try {
                scan = f.openScanTuple();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            if (status == FAIL) {
                System.err.println("*** Error opening scan\n");
            }
        }

        if (status == OK) {
            int len, i = 0;
            DummyRecord rec = null;
            DummyRecord rec2 = null;
            Tuple tuple = new Tuple();
            Tuple tuple2 = new Tuple();
            boolean done = false;

            while (!done) {
                try {
                    tuple = scan.getNextTuple(mid);
                    if (tuple == null) {
                        done = true;
                        break;
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                if (!done && status == OK) {
                    try {
                        rec = new DummyRecord(tuple);
                    } catch (Exception e) {
                        System.err.println("" + e);
                    }

                    // While we're at it, test the getRecord method too.
                    try {
                        tuple2 = f.getRecordTuple(mid);
                    } catch (Exception e) {
                        status = FAIL;
                        System.err.println("*** Error getting record " + i + "\n");
                        e.printStackTrace();
                        break;
                    }

                    try {
                        rec2 = new DummyRecord(tuple2);
                    } catch (Exception e) {
                        System.err.println("" + e);
                        e.printStackTrace();
                    }


                    if ((rec.ival != i) || (rec.fval != (float) i * 7)
                            || (rec2.ival != i) || (rec2.fval != i * 7)) {
                        System.err.println("*** Record " + i
                                + " differs from our update\n");
                        System.err.println("rec.ival: " + rec.ival
                                + " should be " + i + "\n");
                        System.err.println("rec.fval: " + rec.fval
                                + " should be " + (i * 7.0) + "\n");
                        status = FAIL;
                        break;
                    }

                }
                i += 2;     // Because we deleted the odd ones...
            }
        }

        if (status == OK)
            System.out.println("  Test 3 completed successfully.\n");
        return status;

    }

    //deal with variable size records.  it's probably easier to re-write
    //one instead of using the ones from C++
    protected boolean test5() {
        return true;
    }


    protected boolean test4() {

        System.out.println("\n  Test 4: Test some error conditions\n");
        boolean status = OK;
        Scan scan = null;
        MID mid = new MID();
        Heapfile f = null;

        try {
            f = new Heapfile("file_1");
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if (status == OK) {
            System.out.println("  - Try to change the size of a record\n");
            try {
                scan = f.openScanTuple();
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }
        }

        //The following is to test whether tinkering with the size of
        //the tuples will cause any problem.

        if (status == OK) {
            int len;
            DummyRecord rec = null;
            Tuple tuple = new Tuple();

            try {
                tuple = scan.getNextTuple(mid);
                if (tuple == null) {
                    status = FAIL;
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            if (status == FAIL) {
                System.err.println("*** Error reading first record\n");
            }

            if (status == OK) {
                try {
                    rec = new DummyRecord(tuple);
                } catch (Exception e) {
                    System.err.println("" + e);
                    status = FAIL;
                }
                len = tuple.getLength();
                Tuple newTuple = null;
                try {
                    newTuple = new Tuple(rec.toByteArray(), 0, len - 1);
                } catch (Exception e) {
                    System.err.println("" + e);
                    e.printStackTrace();
                }
                try {
                    status = f.updateRecordTuple(mid, newTuple);
                } catch (ChainException e) {
                    status = checkException(e, "heap.InvalidUpdateException");
                    if (status == FAIL) {
                        System.err.println("**** Shortening a record");
                        System.out.println("  --> Failed as expected \n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (status == OK) {
                    status = FAIL;
                    System.err.println("######The expected exception was not thrown\n");
                } else {
                    status = OK;
                }
            }

            if (status == OK) {
                try {
                    rec = new DummyRecord(tuple);
                } catch (Exception e) {
                    System.err.println("" + e);
                    e.printStackTrace();
                }

                len = tuple.getLength();
                Tuple newTuple = null;
                try {
                    newTuple = new Tuple(rec.toByteArray(), 0, len + 1);
                } catch (Exception e) {
                    System.err.println("" + e);
                    e.printStackTrace();
                }
                try {
                    status = f.updateRecordTuple(mid, newTuple);
                } catch (ChainException e) {
                    status = checkException(e, "heap.InvalidUpdateException");
                    if (status == FAIL) {
                        System.err.println("**** Lengthening a record");
                        System.out.println("  --> Failed as expected \n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (status == OK) {
                    status = FAIL;
                    System.err.println("The expected exception was not thrown\n");
                } else {
                    status = OK;
                }
            }
        }

        scan = null;

        if (status == OK) {
            System.out.println("  - Try to insert a record that's too long\n");
            byte[] record = new byte[MINIBASE_PAGESIZE + 4];
            try {
                mid = f.insertRecordTuple(record);
            } catch (ChainException e) {
                status = checkException(e, "heap.SpaceNotAvailableException");
                if (status == FAIL) {
                    System.err.println("**** Inserting a too-long record");
                    System.out.println("  --> Failed as expected \n");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (status == OK) {
                status = FAIL;
                System.err.println("The expected exception was not thrown\n");
            } else {
                status = OK;
            }
        }

        if (status == OK)
            System.out.println("  Test 4 completed successfully.\n");
        return (status == OK);

    }

    public static Map[] generateMaps() throws IOException, InvalidTupleSizeException{
        int choice = 100;
        Map[] mapArr = new Map[choice];
        Map templateMap = new Map();
        /* Variable length map header
            short rowLabelLength = (short)ROW_LABEL.length();
            short columnLabelLength = (short)COL_LABEL.length();
            short valueLength = (short)VALUE.length();
            short fieldCnt = 4;
            m1.setHdr(fieldCnt, null, new short[]{30, 30, 30});
        */
        templateMap.setDefaultHdr();
        for(int i=0; i<choice; i++) {
            Map m = new Map((int)templateMap.getLength());
            byte[] m_data = new byte[(int)templateMap.getLength()];
            m.mapInit(m_data, 0, templateMap.getLength());
            m.setDefaultHdr();
            m.setRowLabel(ROW_LABEL + "-" + Integer.toString(i));
            m.setColumnLabel(COL_LABEL + "-" + Integer.toString(i));
            m.setTimeStamp(TIME_STAMP);
            m.setValue(VALUE + "-" + Integer.toString(i));
            mapArr[i] = m;
        }
        return mapArr;
    }

    protected boolean test6() {
        boolean status = OK;
        try {
            SystemDefs.JavabaseDB.pcounter.initialize();
            System.out.println("\n  Test 6 Map: Insert and scan fixed-size map records\n");
            Map[] mapArr = generateMaps();

            MID mid = new MID();
            Heapfile f = null;

            System.out.println("  - Create a heap file\n");
            try {
                f = new Heapfile("file_1");
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Could not create heap file\n");
                e.printStackTrace();
            }

            if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    != SystemDefs.JavabaseBM.getNumBuffers()) {
                System.err.println("*** The heap file has left pages pinned\n");
                status = FAIL;
            }

            if (status == OK) {
                System.out.println("   - Add " + choice + " records to the file\n");
                for (int i = 0; (i < choice) && (status == OK); i++) {
                    try {
                        mid = f.insertRecordMap(mapArr[i].getMapByteArray());
                    } catch (Exception e) {
                        status = FAIL;
                        System.err.println("*** Error inserting record " + i + "\n");
                        e.printStackTrace();
                    }

                    if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                            != SystemDefs.JavabaseBM.getNumBuffers()) {

                        System.err.println("*** Insertion left a page pinned\n");
                        status = FAIL;
                    }
                }

                try {
                    if (f.getRecCntMap() != choice) {
                        status = FAIL;
                        System.err.println("*** File reports " + f.getRecCntMap() +
                                " records, not " + choice + "\n");
                    }
                } catch (Exception e) {
                    status = FAIL;
                    System.out.println("" + e);
                    e.printStackTrace();
                }
            }
            // In general, a sequential scan won't be in the same order as the
            // insertions.  However, we're inserting fixed-length records here, and
            // in this case the scan must return the insertion order.

            Scan scan = null;

            if (status == OK) {
                System.out.println("   - Scan the records just inserted\n");

                try {
                    scan = f.openScanMap();
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Error opening scan\n");
                    e.printStackTrace();
                }

                if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                        == SystemDefs.JavabaseBM.getNumBuffers()) {
                    System.err.println("*** The heap-file scan has not pinned the first page\n");
                    status = FAIL;
                }
            }

            if (status == OK) {
                int i = 0;
                Map map = new Map();

                boolean done = false;
                while (!done) {
                    try {
                        map = scan.getNextMap(mid);
                        if (map == null) {
                            done = true;
                            break;
                        }
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    if (status == OK && !done) {
                        try {
                            // map.setHdr(fieldCnt, null, new short[]{rowLabelLength, columnLabelLength, valueLength})
                            map.setFldOffset(map.getMapByteArray());
                            if(!MapUtils.Equal(map, mapArr[i])){
                                System.err.println("*** Record " + i
                                        + " differs from what we inserted\n");
                                System.err.println("Row label should be " + mapArr[i].getRowLabel() + " but is " + map.getRowLabel());
                                System.err.println("Column label should be " + mapArr[i].getRowLabel() + " but is " + map.getRowLabel());
                                System.err.println("TimeStamp should be " + mapArr[i].getRowLabel() + " but is " + map.getRowLabel());
                                System.err.println("Value should be " + mapArr[i].getRowLabel() + " but is " + map.getRowLabel());
                            }
                        } catch (Exception e) {
                            System.err.println("" + e);
                            e.printStackTrace();
                        }
                    }
                    ++i;
                }

                //If it gets here, then the scan should be completed
                if (status == OK) {
                    if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                            != SystemDefs.JavabaseBM.getNumBuffers()) {
                        System.err.println("*** The heap-file scan has not unpinned " +
                                "its page after finishing\n");
                        status = FAIL;
                    } else if (i != (choice)) {
                        status = FAIL;

                        System.err.println("*** Scanned " + i + " records instead of "
                                + choice + "\n");
                    }
                }
            }

            if (status == OK)
            System.out.println("  Test 6: Map record insertion completed successfully");
            int read_counter = SystemDefs.JavabaseDB.pcounter.getRCounter();
            int write_counter = SystemDefs.JavabaseDB.pcounter.getWCounter();
            System.out.println("Read Counter: " + read_counter);
            System.out.println("Write Counter: " + write_counter);
        } catch (Exception e) {
            System.out.println(e);
            status = FAIL;
        }
        return status;
    }

    protected boolean test7() {

        System.out.println("\n  Test 7: Delete map record\n");
        SystemDefs.JavabaseDB.pcounter.initialize();
        boolean status = OK;
        Scan scan = null;
        MID mid = new MID();
        Heapfile f = null;

        System.out.println("  - Open the same heap file as test 6\n");
        try {
            f = new Heapfile("file_1");
        } catch (Exception e) {
            status = FAIL;
            System.err.println(" Could not open heapfile");
            e.printStackTrace();
        }

        if (status == OK) {
            System.out.println("  - Delete half the records\n");
            try {
                scan = f.openScanMap();
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }
        }

        if (status == OK) {
            try {
                int len, i = 0;
                Map map = null;
                boolean done = false;

                while (!done) {
                    try {
                        map = scan.getNextMap(mid);
                        if (map == null) {
                            done = true;
                        }
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        break;
                    }

                    if (!done && status == OK) {
                        boolean odd = true;
                        if (i % 2 == 1) odd = true;
                        if (i % 2 == 0) odd = false;
                        if (odd) {       // Delete the odd-numbered ones.
                            try {
                                status = f.deleteRecordMap(mid);
                            } catch (Exception e) {
                                status = FAIL;
                                System.err.println("*** Error deleting record " + i + "\n");
                                e.printStackTrace();
                                break;
                            }
                        }
                    }
                    ++i;
                }
            } catch (Exception e) {
                status = FAIL;
                System.out.println(e);
            }
        }

        scan.closescan();    //  destruct scan!!!!!!!!!!!!!!!
        scan = null;

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {

            System.out.println("\nt2: in if: Number of unpinned buffers: "
                    + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "\n");
            System.err.println("t2: in if: getNumbfrs: " + SystemDefs.JavabaseBM.getNumBuffers() + "\n");

            System.err.println("*** Deletion left a page pinned\n");
            status = FAIL;
        }

        if (status == OK) {
            System.out.println("  - Scan the remaining records\n");
            try {
                scan = f.openScanMap();
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }
        }

        if (status == OK) {
            try {
                int len, i = 0;
                Map map = new Map();
                boolean done = false;

                while (!done) {
                    try {
                        map = scan.getNextMap(mid);
                        if (map == null) {
                            done = true;
                        }
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        break;
                    }

                    if (!done && status == OK) {
                        try {
                            map.setFldOffset(map.getMapByteArray());
                            String expectedRowLabel = ROW_LABEL + "-" + Integer.toString(i);
                            String expectedColLabel = COL_LABEL + "-" + Integer.toString(i);
                            int expectedTimestamp = TIME_STAMP;
                            String expectedValue = VALUE + "-" + Integer.toString(i);
                            if(!(map.getRowLabel().equals(expectedRowLabel) && map.getColumnLabel().equals(expectedColLabel)
                            && map.getTimeStamp() == expectedTimestamp && map.getValue().equals(expectedValue))) {
                                status = FAIL;
                                break;
                            }
                        } catch (Exception e) {
                            System.err.println("" + e);
                            e.printStackTrace();
                        }
                        i += 2;     // Because we deleted the odd ones...
                    }
                }
                System.out.println("  - No of records left :" +f.getRecCntMap());
            } catch (Exception e) {
                status = FAIL;
                System.out.println(e);
            }
        }

        if (status == OK)
            System.out.println("  Test 7 completed successfully.\n");
        int read_counter = SystemDefs.JavabaseDB.pcounter.getRCounter();
        int write_counter = SystemDefs.JavabaseDB.pcounter.getWCounter();
        System.out.println("Read Counter: " + read_counter);
        System.out.println("Write Counter: " + write_counter);
        return status;

    }

    protected boolean test8() {

        System.out.println("\n  Test 8: Update map records\n");
        boolean status = OK;
        Scan scan = null;
        MID mid = new MID();
        Heapfile f = null;

        System.out.println("  - Open the same heap file as tests 6 and 7\n");
        try {
            f = new Heapfile("file_1");
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if (status == OK) {
            System.out.println("  - Change the records\n");
            try {
                scan = f.openScanMap();
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }
        }

        if (status == OK) {
            try {
                int i = 0;
                Map map = null;
                boolean done = false;

                while (!done) {
                    try {
                        map = scan.getNextMap(mid);
                        if (map == null) {
                            done = true;
                        }
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        break;
                    }

                    if (!done && status == OK) {

                        Map newMap = null;
                        try {
                            newMap = new Map(map.getMapByteArray(), 0, map.getLength());
                            newMap.setFldOffset(map.getMapByteArray());
                            String updatedRowLabel = newMap.getRowLabel() + "-mod";
                            String updatedColLabel = newMap.getColumnLabel() + "-mod";
                            String updatedValue = newMap.getValue() + "-mod";
                            int updatedTimestamp = newMap.getTimeStamp();

                            newMap.setDefaultHdr();
                            newMap.setRowLabel(updatedRowLabel);
                            newMap.setColumnLabel(updatedColLabel);
                            newMap.setTimeStamp(updatedTimestamp);
                            newMap.setValue(updatedValue);
                        } catch (Exception e) {
                            status = FAIL;
                            System.err.println("" + e);
                            e.printStackTrace();
                        }
                        try {
                            status = f.updateRecordMap(mid, newMap);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        if (status != OK) {
                            System.err.println("*** Error updating record " + i + "\n");
                            break;
                        }
                        i += 2;     // Recall, we deleted every other record above.
                    }
                }
            } catch (Exception e) {
                status = FAIL;
                System.out.println(e);
            }
        }

        scan = null;

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {


            System.out.println("t3, Number of unpinned buffers: "
                    + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "\n");
            System.err.println("t3, getNumbfrs: " + SystemDefs.JavabaseBM.getNumBuffers() + "\n");

            System.err.println("*** Updating left pages pinned\n");
            status = FAIL;
        }

        if (status == OK) {
            System.out.println("  - Check that the updates are really there\n");
            try {
                scan = f.openScanMap();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            if (status == FAIL) {
                System.err.println("*** Error opening scan\n");
            }
        }

        if (status == OK) {
            try {
                int len, i = 0;
                Map map1 = new Map();
                Map map2;
                boolean done = false;

                while (!done) {
                    try {
                        map1 = scan.getNextMap(mid);
                        if (map1 == null) {
                            done = true;
                            break;
                        }
                        map1.setFldOffset(map1.getMapByteArray());
                        String updatedRowLabel = ROW_LABEL + "-" + Integer.toString(i) + "-mod";
                        String updatedColLabel = COL_LABEL + "-" + Integer.toString(i)+ "-mod";
                        String updatedValue = VALUE + "-" + Integer.toString(i) + "-mod";
                        int updatedTimestamp = TIME_STAMP;

                        if(!(map1.getRowLabel().equals(updatedRowLabel) && map1.getColumnLabel().equals(updatedColLabel)
                                && map1.getTimeStamp() == updatedTimestamp && map1.getValue().equals(updatedValue))) {
                            status = FAIL;
                            break;
                        }
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        break;
                    }

                    if (!done && status == OK) {

                        // While we're at it, test the getRecord method too.
                        try {
                            map2 = f.getRecordMap(mid);
                            map2.setFldOffset(map2.getMapByteArray());
                        } catch (Exception e) {
                            status = FAIL;
                            System.err.println("*** Error getting record " + i + "\n");
                            e.printStackTrace();
                            break;
                        }

                        if(!MapUtils.Equal(map1, map2)){
                            status = FAIL;
                            System.err.println("*** Record " + i
                                    + " differs from what we updated\n");
                            System.err.println("Row label should be " + map2.getRowLabel() + " but is " + map1.getRowLabel());
                            System.err.println("Column label should be " + map2.getRowLabel() + " but is " + map1.getRowLabel());
                            System.err.println("TimeStamp should be " + map2.getRowLabel() + " but is " + map1.getRowLabel());
                            System.err.println("Value should be " + map2.getRowLabel() + " but is " + map1.getRowLabel());
                            break;
                        }

                    }
                    i += 2;     // Because we deleted the odd ones...
                }
                scan.closescan();
            }catch(Exception e){
                status = FAIL;
                System.out.println(e);
            }
        }
        try {
            scan = f.openScanMap();
            if (status == OK) {
                int i = 0;
                Map map = new Map();
                mid = new MID();
                boolean done = false;
                while (!done) {
                    try {
                        map = scan.getNextMap(mid);
                        if (map == null) {
                            done = true;
                            break;
                        }
                        f.deleteRecordMap(mid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        break;
                    }
                    ++i;
                }
            }

            if(f.getRecCntMap() != 0) {
                status = FAIL;
                System.out.println(f.getRecCntMap()+" maps are still left in the heap file!");
            }
        } catch (Exception e) {
            status = FAIL;
            System.out.println(e);
        }
        if (status == OK)
            System.out.println("  Test 8 completed successfully.\n");
        return status;
    }

    protected Map[] generateSimilarMaps() throws IOException, InvalidTupleSizeException{
        Map[] mapArr = new Map[4];
        Map templateMap = new Map();
        /* Variable length map header
            short rowLabelLength = (short)ROW_LABEL.length();
            short columnLabelLength = (short)COL_LABEL.length();
            short valueLength = (short)VALUE.length();
            short fieldCnt = 4;
            m1.setHdr(fieldCnt, null, new short[]{30, 30, 30});
        */
        templateMap.setDefaultHdr();
        for(int i=0; i<4; i++) {
            Map m = new Map((int)templateMap.getLength());
            byte[] m_data = new byte[(int)templateMap.getLength()];
            m.mapInit(m_data, 0, templateMap.getLength());
            m.setDefaultHdr();
            m.setRowLabel(ROW_LABEL);
            m.setColumnLabel(COL_LABEL);
            m.setTimeStamp(TIME_STAMP + i);
            m.setValue(Integer.toString(i));
            mapArr[i] = m;
        }
        return mapArr;
    }

    protected Map[] generateExpectedMap() throws IOException, InvalidTupleSizeException {
        Map[] mapArr = new Map[3];
        Map templateMap = new Map();
        /* Variable length map header
            short rowLabelLength = (short)ROW_LABEL.length();
            short columnLabelLength = (short)COL_LABEL.length();
            short valueLength = (short)VALUE.length();
            short fieldCnt = 4;
            m1.setHdr(fieldCnt, null, new short[]{30, 30, 30});
        */
        templateMap.setDefaultHdr();
        for(int i=0; i<3; i++) {
            Map m = new Map((int)templateMap.getLength());
            byte[] m_data = new byte[(int)templateMap.getLength()];
            m.mapInit(m_data, 0, templateMap.getLength());
            m.setDefaultHdr();
            m.setRowLabel(ROW_LABEL);
            m.setColumnLabel(COL_LABEL);
            if(i == 0) {
                m.setTimeStamp(TIME_STAMP + i+3);
                m.setValue(Integer.toString(i+3));
            } else {
                m.setTimeStamp(TIME_STAMP + i);
                m.setValue(Integer.toString(i));
            }

            mapArr[i] = m;
        }
        return mapArr;
    }

    protected boolean test9() {
        boolean status = OK;
        try {
            System.out.println("\n  Test 9 Map: Insert similar map records and validate that only 3 map records are stored. \n");
            Map[] mapArr = generateSimilarMaps();
            Map[] expectedMapArr = generateExpectedMap();

            MID mid = new MID();
            Heapfile f = null;

            System.out.println("  - Create a heap file\n");
            try {
                f = new Heapfile("file_1");
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Could not create heap file\n");
                e.printStackTrace();
            }

            if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    != SystemDefs.JavabaseBM.getNumBuffers()) {
                System.err.println("*** The heap file has left pages pinned\n");
                status = FAIL;
            }

            if (status == OK) {
                System.out.println("   - Add " + 4 + " records to the file\n");
                for (int i = 0; (i < 4) && (status == OK); i++) {
                    try {
                        mid = f.insertRecordMapWithoutIndex(mapArr[i].getMapByteArray());
                    } catch (Exception e) {
                        status = FAIL;
                        System.err.println("*** Error inserting record " + i + "\n");
                        e.printStackTrace();
                    }

                    if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                            != SystemDefs.JavabaseBM.getNumBuffers()) {

                        System.err.println("*** Insertion left a page pinned\n");
                        status = FAIL;
                    }
                }

                try {
                    if (f.getRecCntMap() != 3) {
                        status = FAIL;
                        System.err.println("*** File reports " + f.getRecCntMap() +
                                " records, not " + 3 + "\n");
                    }
                } catch (Exception e) {
                    status = FAIL;
                    System.out.println("" + e);
                    e.printStackTrace();
                }
            }
            // In general, a sequential scan won't be in the same order as the
            // insertions.  However, we're inserting fixed-length records here, and
            // in this case the scan must return the insertion order.

            Scan scan = null;

            if (status == OK) {
                System.out.println("   - Scan the records just inserted\n");

                try {
                    scan = f.openScanMap();
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Error opening scan\n");
                    e.printStackTrace();
                }

                if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                        == SystemDefs.JavabaseBM.getNumBuffers()) {
                    System.err.println("*** The heap-file scan has not pinned the first page\n");
                    status = FAIL;
                }
            }

            if (status == OK) {
                int i = 0;
                Map map = new Map();

                boolean done = false;
                while (!done) {
                    try {
                        map = scan.getNextMap(mid);
                        if (map == null) {
                            done = true;
                            break;
                        }
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    if (status == OK && !done) {
                        try {
                            // map.setHdr(fieldCnt, null, new short[]{rowLabelLength, columnLabelLength, valueLength})
                            map.setFldOffset(map.getMapByteArray());
                            map.print();
                            if(!MapUtils.Equal(map, expectedMapArr[i])){
                                System.err.println("*** Record " + i
                                        + " differs from what we inserted\n");
                                System.err.println("Row label should be " + expectedMapArr[i].getRowLabel() + " but is " + map.getRowLabel());
                                System.err.println("Column label should be " + expectedMapArr[i].getColumnLabel() + " but is " + map.getColumnLabel());
                                System.err.println("TimeStamp should be " + expectedMapArr[i].getTimeStamp() + " but is " + map.getTimeStamp());
                                System.err.println("Value should be " + expectedMapArr[i].getValue() + " but is " + map.getValue());
                            }
                        } catch (Exception e) {
                            System.err.println("" + e);
                            e.printStackTrace();
                        }
                    }
                    ++i;
                }

                //If it gets here, then the scan should be completed
                if (status == OK) {
                    if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                            != SystemDefs.JavabaseBM.getNumBuffers()) {
                        System.err.println("*** The heap-file scan has not unpinned " +
                                "its page after finishing\n");
                        status = FAIL;
                    } else if (i != (3)) {
                        status = FAIL;

                        System.err.println("*** Scanned " + i + " records instead of "
                                + 3 + "\n");
                    }
                }
            }

            if (status == OK)
                System.out.println("  Test 9: Similar map records were inserted successfully.");
        } catch (Exception e) {
            System.out.println(e);
            status = FAIL;
        }
        return status;
    }

    protected boolean runAllTests() {

        boolean _passAll = OK;

//        if (!test1()) {
//            _passAll = FAIL;
//        }
//
//        if (!test2()) {
//            _passAll = FAIL;
//        }
//        if (!test3()) {
//            _passAll = FAIL;
//        }
//        if (!test4()) {
//            _passAll = FAIL;
//        }
//        if (!test5()) {
//            _passAll = FAIL;
//        }
        if (!test6()) {
            _passAll = FAIL;
        }
        if (!test7()) {
            _passAll = FAIL;
        }
        if (!test8()) {
            _passAll = FAIL;
        }
        if (!test9()) {
            _passAll = FAIL;
        }
        return _passAll;

    }

    protected String testName() {

        return "Heap File";
    }
}

// This is added to substitute the struct construct in C++
class DummyRecord {

    //content of the record
    public int ival;
    public float fval;
    public String name;

    //length under control
    private int reclen;

    private byte[] data;

    /**
     * Default constructor
     */
    public DummyRecord() {
    }

    /**
     * another constructor
     */
    public DummyRecord(int _reclen) {
        setRecLen(_reclen);
        data = new byte[_reclen];
    }

    /**
     * constructor: convert a byte array to DummyRecord object.
     *
     * @param arecord a byte array which represents the DummyRecord object
     */
    public DummyRecord(byte[] arecord)
            throws java.io.IOException {
        setIntRec(arecord);
        setFloRec(arecord);
        setStrRec(arecord);
        data = arecord;
        setRecLen(name.length());
    }

    /**
     * constructor: translate a tuple to a DummyRecord object
     * it will make a copy of the data in the tuple
     *
     * @param atuple: the input tuple
     */
    public DummyRecord(Tuple _atuple)
            throws java.io.IOException {
        data = new byte[_atuple.getLength()];
        data = _atuple.getTupleByteArray();
        setRecLen(_atuple.getLength());

        setIntRec(data);
        setFloRec(data);
        setStrRec(data);

    }

    /**
     * convert this class objcet to a byte array
     * this is used when you want to write this object to a byte array
     */
    public byte[] toByteArray()
            throws java.io.IOException {
        //    data = new byte[reclen];
        Convert.setIntValue(ival, 0, data);
        Convert.setFloValue(fval, 4, data);
        Convert.setStrValue(name, 8, data);
        return data;
    }

    /**
     * get the integer value out of the byte array and set it to
     * the int value of the DummyRecord object
     */
    public void setIntRec(byte[] _data)
            throws java.io.IOException {
        ival = Convert.getIntValue(0, _data);
    }

    /**
     * get the float value out of the byte array and set it to
     * the float value of the DummyRecord object
     */
    public void setFloRec(byte[] _data)
            throws java.io.IOException {
        fval = Convert.getFloValue(4, _data);
    }

    /**
     * get the String value out of the byte array and set it to
     * the float value of the HTDummyRecorHT object
     */
    public void setStrRec(byte[] _data)
            throws java.io.IOException {
        // System.out.println("reclne= "+reclen);
        // System.out.println("data size "+_data.size());
        name = Convert.getStrValue(8, _data, reclen - 8);
    }

    //Other access methods to the size of the String field and
    //the size of the record
    public void setRecLen(int size) {
        reclen = size;
    }

    public int getRecLength() {
        return reclen;
    }
}

public class HFTest {

    public static void main(String argv[]) {

        HFDriver hd = new HFDriver();
        boolean dbstatus;

        dbstatus = hd.runTests();

        if (dbstatus != true) {
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}
