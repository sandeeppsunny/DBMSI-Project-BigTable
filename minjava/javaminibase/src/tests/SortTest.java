package tests;

import java.io.*;

import BigT.Map;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;

import java.util.Random;


class SORTDriver extends TestDriver
        implements GlobalConst {

    private static String data1[] = {
            "raghu", "xbao", "cychan", "leela", "ketola", "soma", "ulloa",
            "dhanoa", "dsilva", "kurniawa", "dissoswa", "waic", "susanc", "kinc",
            "marc", "scottc", "yuc", "ireland", "rathgebe", "joyce", "daode",
            "yuvadee", "he", "huxtable", "muerle", "flechtne", "thiodore", "jhowe",
            "frankief", "yiching", "xiaoming", "jsong", "yung", "muthiah", "bloch",
            "binh", "dai", "hai", "handi", "shi", "sonthi", "evgueni", "chung-pi",
            "chui", "siddiqui", "mak", "tak", "sungk", "randal", "barthel",
            "newell", "schiesl", "neuman", "heitzman", "wan", "gunawan", "djensen",
            "juei-wen", "josephin", "harimin", "xin", "zmudzin", "feldmann",
            "joon", "wawrzon", "yi-chun", "wenchao", "seo", "karsono", "dwiyono",
            "ginther", "keeler", "peter", "lukas", "edwards", "mirwais", "schleis",
            "haris", "meyers", "azat", "shun-kit", "robert", "markert", "wlau",
            "honghu", "guangshu", "chingju", "bradw", "andyw", "gray", "vharvey",
            "awny", "savoy", "meltz"};

    private static String data2[] = {
            "andyw", "awny", "azat", "barthel", "binh", "bloch", "bradw",
            "chingju", "chui", "chung-pi", "cychan", "dai", "daode", "dhanoa",
            "dissoswa", "djensen", "dsilva", "dwiyono", "edwards", "evgueni",
            "feldmann", "flechtne", "frankief", "ginther", "gray", "guangshu",
            "gunawan", "hai", "handi", "harimin", "haris", "he", "heitzman",
            "honghu", "huxtable", "ireland", "jhowe", "joon", "josephin", "joyce",
            "jsong", "juei-wen", "karsono", "keeler", "ketola", "kinc", "kurniawa",
            "leela", "lukas", "mak", "marc", "markert", "meltz", "meyers",
            "mirwais", "muerle", "muthiah", "neuman", "newell", "peter", "raghu",
            "randal", "rathgebe", "robert", "savoy", "schiesl", "schleis",
            "scottc", "seo", "shi", "shun-kit", "siddiqui", "soma", "sonthi",
            "sungk", "susanc", "tak", "thiodore", "ulloa", "vharvey", "waic",
            "wan", "wawrzon", "wenchao", "wlau", "xbao", "xiaoming", "xin",
            "yi-chun", "yiching", "yuc", "yung", "yuvadee", "zmudzin"};

    private static int NUM_RECORDS = data2.length;
    private static int LARGE = 1000;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private static int SORTPGNUM = 12;


    public SORTDriver() {
        super("sorttest");
    }

    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

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
            System.err.println("" + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        System.out.println("\n" + "..." + testName() + " tests ");
        System.out.println(_pass == OK ? "completely successfully" : "failed");
        System.out.println(".\n\n");

        return _pass;
    }

    protected boolean test1() {
        System.out.println("------------------------ TEST 1 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        short[] attrSize = new short[2];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        MID mid;
        Heapfile f = null;
        try {
            f = new Heapfile("test1.in");
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < NUM_RECORDS; i++) {
            try {
                t.setStrFld(1, data1[i]);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                mid = f.insertRecordTuple(t.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);

        FileScan fscan = null;

        try {
            fscan = new FileScan("test1.in", attrType, attrSize, (short) 2, 2, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1.in"
        Sort sort = null;
        try {
            sort = new Sort(attrType, (short) 2, attrSize, fscan, 1, order[0], REC_LEN1, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        int count = 0;
        t = null;
        String outval = null;

        try {
            t = sort.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;

        while (t != null) {
            if (count >= NUM_RECORDS) {
                System.err.println("Test1 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval = t.getStrFld(1);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (outval.compareTo(data2[count]) != 0) {
                System.err.println("outval = " + outval + "\tdata2[count] = " + data2[count]);

                System.err.println("Test1 -- OOPS! test1.out not sorted");
                status = FAIL;
            }
            count++;

            try {
                t = sort.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (count < NUM_RECORDS) {
            System.err.println("Test1 -- OOPS! too few records");
            status = FAIL;
        } else if (flag && status) {
            System.err.println("Test1 -- Sorting OK");
        }

        // clean up
        try {
            sort.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.err.println("------------------- TEST 1 completed ---------------------\n");

        return status;
    }


    protected boolean test2() {
        System.out.println("------------------------ TEST 2 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[1];
        attrType[0] = new AttrType(AttrType.attrString);
        short[] attrSize = new short[1];
        attrSize[0] = REC_LEN1;
        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 1, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        int size = t.size();

        // Create unsorted data file "test2.in"
        MID mid;
        Heapfile f = null;
        try {
            f = new Heapfile("test2.in");
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 1, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < NUM_RECORDS; i++) {
            try {
                t.setStrFld(1, data1[i]);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                mid = f.insertRecordTuple(t.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);

        FileScan fscan = null;

        try {
            fscan = new FileScan("test2.in", attrType, attrSize, (short) 1, 1, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test2.in"
        Sort sort = null;
        try {
            sort = new Sort(attrType, (short) 1, attrSize, fscan, 1, order[1], REC_LEN1, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        int count = 0;
        t = null;
        String outval = null;

        try {
            t = sort.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;

        while (t != null) {
            if (count >= NUM_RECORDS) {
                System.err.println("Test2 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval = t.getStrFld(1);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (outval.compareTo(data2[NUM_RECORDS - count - 1]) != 0) {
                System.err.println("Test2 -- OOPS! test2.out not sorted");
                status = FAIL;
            }
            count++;

            try {
                t = sort.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (count < NUM_RECORDS) {
            System.err.println("Test2 -- OOPS! too few records");
            status = FAIL;
        } else if (flag && status) {
            System.err.println("Test2 -- Sorting OK");
        }

        // clean up
        try {
            sort.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.err.println("------------------- TEST 2 completed ---------------------\n");

        return status;
    }


    protected boolean test3() {
        System.out.println("------------------------ TEST 3 --------------------------");

        boolean status = OK;

        Random random1 = new Random((long) 1000);
        Random random2 = new Random((long) 1000);

        AttrType[] attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrReal);
        short[] attrSize = new short[2];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN1;
        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        Tuple t = new Tuple();

        try {
            t.setHdr((short) 4, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test3.in"
        MID mid;
        Heapfile f = null;
        try {
            f = new Heapfile("test3.in");
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 4, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int inum = 0;
        float fnum = 0;
        int count = 0;

        for (int i = 0; i < LARGE; i++) {
            // setting fields
            inum = random1.nextInt();
            fnum = random2.nextFloat();
            try {
                t.setStrFld(1, data1[i % NUM_RECORDS]);
                t.setIntFld(3, inum);
                t.setFloFld(4, fnum);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                mid = f.insertRecordTuple(t.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);

        FileScan fscan = null;

        // Sort "test3.in" on the int attribute (field 3) -- Ascending
        System.out.println(" -- Sorting in ascending order on the int field -- ");

        try {
            fscan = new FileScan("test3.in", attrType, attrSize, (short) 4, 4, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        Sort sort = null;
        try {
            sort = new Sort(attrType, (short) 4, attrSize, fscan, 3, order[0], 4, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        count = 0;
        t = null;
        int iout = 0;
        int ival = 0;

        try {
            t = sort.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        if (t != null) {
            // get an initial value
            try {
                ival = t.getIntFld(3);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        boolean flag = true;

        while (t != null) {
            if (count >= LARGE) {
                System.err.println("Test3 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                iout = t.getIntFld(3);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (iout < ival) {
                System.err.println("count = " + count + " iout = " + iout + " ival = " + ival);

                System.err.println("Test3 -- OOPS! test3.out not sorted");
                status = FAIL;
                break;
            }
            count++;
            ival = iout;

            try {
                t = sort.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (count < LARGE) {
            System.err.println("Test3 -- OOPS! too few records");
            status = FAIL;
        } else if (flag && status) {
            System.err.println("Test3 -- Sorting of int field OK\n");
        }

        // clean up
        try {
            sort.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test3.in" on the int attribute (field 3) -- Ascending
        System.out.println(" -- Sorting in descending order on the float field -- ");

        try {
            fscan = new FileScan("test3.in", attrType, attrSize, (short) 4, 4, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            sort = new Sort(attrType, (short) 4, attrSize, fscan, 4, order[1], 4, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        count = 0;
        t = null;
        float fout = 0;
        float fval = 0;

        try {
            t = sort.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        if (t != null) {
            // get an initial value
            try {
                fval = t.getFloFld(4);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        flag = true;

        while (t != null) {
            if (count >= LARGE) {
                System.err.println("Test3 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                fout = t.getFloFld(4);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (fout > fval) {
                System.err.println("count = " + count + " fout = " + fout + " fval = " + fval);

                System.err.println("Test3 -- OOPS! test3.out not sorted");
                status = FAIL;
                break;
            }
            count++;
            fval = fout;

            try {
                t = sort.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (count < LARGE) {
            System.err.println("Test3 -- OOPS! too few records");
            status = FAIL;
        } else if (flag && status) {
            System.err.println("Test3 -- Sorting of float field OK\n");
        }

        // clean up
        try {
            sort.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.err.println("------------------- TEST 3 completed ---------------------\n");

        return status;
    }

    protected boolean test4() {
        System.out.println("------------------------ TEST 4 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        short[] attrSize = new short[2];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        int size = t.size();

        // Create unsorted data file
        MID rid1, rid2;
        Heapfile f1 = null;
        Heapfile f2 = null;
        try {
            f1 = new Heapfile("test4-1.in");
            f2 = new Heapfile("test4-2.in");
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < NUM_RECORDS; i++) {
            try {
                t.setStrFld(1, data1[i]);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid1 = f1.insertRecordTuple(t.returnTupleByteArray());
                rid2 = f2.insertRecordTuple(t.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }


        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);

        FileScan fscan1 = null;
        FileScan fscan2 = null;

        try {
            fscan1 = new FileScan("test4-1.in", attrType, attrSize, (short) 2, 2, projlist, null);
            fscan2 = new FileScan("test4-2.in", attrType, attrSize, (short) 2, 2, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        // Sort input files
        Sort sort1 = null;
        Sort sort2 = null;
        try {
            sort1 = new Sort(attrType, (short) 2, attrSize, fscan1, 1, order[0], REC_LEN1, SORTPGNUM);
            sort2 = new Sort(attrType, (short) 2, attrSize, fscan2, 1, order[1], REC_LEN1, SORTPGNUM);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        int count = 0;
        Tuple t1 = null;
        Tuple t2 = null;
        String outval = null;

        try {
            t1 = sort1.get_next();
            t2 = sort2.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;

        while (t1 != null) {
            if (count >= NUM_RECORDS) {
                System.err.println("Test4 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval = t1.getStrFld(1);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (outval.compareTo(data2[count]) != 0) {
                System.err.println("outval = " + outval + "\tdata2[count] = " + data2[count]);

                System.err.println("Test4 -- OOPS! test4.out not sorted");
                status = FAIL;
            }
            count++;

            if (t2 == null) {
                System.err.println("Test4 -- t2 is null prematurely");
                status = FAIL;
            } else {
                try {
                    outval = t2.getStrFld(1);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                if (outval.compareTo(data2[NUM_RECORDS - count]) != 0) {
                    System.err.println("outval = " + outval + "\tdata2[count] = " + data2[NUM_RECORDS - count]);

                    System.err.println("Test4 -- OOPS! test4.out not sorted");
                    status = FAIL;
                }
            }

            try {
                t1 = sort1.get_next();
                t2 = sort2.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (count < NUM_RECORDS) {
            System.err.println("count = " + count);

            System.err.println("Test4 -- OOPS! too few records");
            status = FAIL;
        } else if (flag && status) {
            System.err.println("Test4 -- Sorting OK");
        }

        // clean up
        try {
            sort1.close();
            sort2.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.err.println("------------------- TEST 4 completed ---------------------\n");

        return status;
    }

    protected boolean test5() {
        System.out.println("------------------------ TEST 5 --------------------------");
        boolean status = OK;
        try {
            AttrType[] attrType = new AttrType[2];
            attrType[0] = new AttrType(AttrType.attrString);
            attrType[1] = new AttrType(AttrType.attrString);
            short[] attrSize = new short[2];
            attrSize[0] = 30;
            attrSize[1] = 30;
            MapOrder[] order = new MapOrder[2];
            order[0] = new MapOrder(TupleOrder.Ascending);
            order[1] = new MapOrder(TupleOrder.Descending);

            // create a tuple of appropriate size
            Map m = new Map();
            try {
                m.setDefaultHdr();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            int size = m.size();

            // Create unsorted data file "test1.in"
            MID mid;
            Heapfile f = null;
            try {
                f = new Heapfile("test7.in");
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }


            for (int i = 0; i < NUM_RECORDS; i++) {
                Map s = new Map((int)m.getLength());
                try {
                    byte[] m_data = new byte[(int)m.getLength()];
                    s.mapInit(m_data, 0, m.getLength());
                    s.setDefaultHdr();
                    s.setRowLabel(data2[i]);
                    s.setColumnLabel(data1[i]);
                    s.setTimeStamp(i);
                    s.setValue(Integer.toString(i));
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                try {
                    mid = f.insertRecordMap(s.returnMapByteArray());
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                    return status;
                }
            }

            // create an iterator by open a file scan
            FldSpec[] projlist = new FldSpec[2];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 0);
            projlist[1] = new FldSpec(rel, 1);

            // set up an identity selection
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopGE);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].operand2.string = "andyw";
            expr[0].next = null;
            expr[1] = new CondExpr();
            expr[1].op = new AttrOperator(AttrOperator.aopLE);
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[1].operand2.string = "randal";
            expr[1].next = null;
            expr[2] = null;

            FileScanMap fscan = null;

            try {
                fscan = new FileScanMap("test7.in", projlist, expr);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            while(true) {
                Map a = fscan.get_next();
                if(a == null) {
                    break;
                }
                a.setFldOffset(a.getMapByteArray());
                a.print();
            }


            System.err.println("------------------- TEST 5 completed ---------------------\n");

            return status;
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
    }


    protected boolean test6() {
        System.out.println("------------------------ TEST 6 --------------------------");
        boolean status = OK;
        try {
            MapOrder[] order = new MapOrder[2];
            order[0] = new MapOrder(TupleOrder.Ascending);
            order[1] = new MapOrder(TupleOrder.Descending);

            // create a tuple of appropriate size
            Map m = new Map();
            try {
                m.setDefaultHdr();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            int size = m.size();

            // Create unsorted data file "test1.in"
            MID mid;
            Heapfile f = null;
            try {
                f = new Heapfile("test8.in");
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }


            for (int i = 0; i < NUM_RECORDS; i++) {
                Map s = new Map((int)m.getLength());
                try {
                    byte[] m_data = new byte[(int)m.getLength()];
                    s.mapInit(m_data, 0, m.getLength());
                    s.setDefaultHdr();
                    s.setRowLabel(data1[i]);
                    s.setColumnLabel(data2[i]);
                    s.setTimeStamp(i);
                    s.setValue(Integer.toString(i));
                    try {
                        mid = f.insertRecordMap(s.returnMapByteArray());
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        return status;
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            // create an iterator by open a file scan
            FldSpec[] projlist = new FldSpec[2];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 0);
            projlist[1] = new FldSpec(rel, 1);

            // set up an identity selection
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopGE);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].operand2.string = "andyw";
            expr[0].next = null;
            expr[1] = new CondExpr();
            expr[1].op = new AttrOperator(AttrOperator.aopLE);
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[1].operand2.string = "randal";
            expr[1].next = null;
            expr[2] = null;

            // Sort in ascending order on row label
            System.out.println("\n==============================");
            System.out.println("Ascending order on row label\n");
            FileScanMap fscan = null;

            try {
                fscan = new FileScanMap("test8.in", projlist, expr);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            // Sort "test8.in"
            SortMap sort = null;
            try {
                sort = new SortMap(null, null, null, fscan, 1, order[0], null, SORTPGNUM);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            Map t = null;

            int i=0;
            while(true) {
                try {
                    t = sort.get_next();
                    if(t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();

                    if(!t.getRowLabel().equals(data2[i])) {
                        System.out.println("Unexpected row value after sorting!");
                        System.out.println("Expected :" + data2[i] + " but got " + t.getRowLabel());
                        break;
                    }
                    i++;
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                    break;
                }
            }

            // Sort in descending order on row label
            try {
                fscan = new FileScanMap("test8.in", projlist, expr);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            try {
                sort = new SortMap(null, null, null, fscan, 1, order[1], null, SORTPGNUM);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            i=61;
            System.out.println("\n==============================");
            System.out.println("Descending order on row label\n");
            while(true) {
                try {
                    t = sort.get_next();
                    if(t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();

                    if(!t.getRowLabel().equals(data2[i])) {
                        System.out.println("Unexpected row value after sorting!");
                        System.out.println("Expected :" + data2[i] + " but got " + t.getRowLabel());
                        break;
                    }
                    i--;
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                    break;
                }
            }

            // Sort in ascending order on timestamp
            try {
                fscan = new FileScanMap("test8.in", projlist, expr);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            try {
                sort = new SortMap(null, null, null, fscan, 5, order[0], null, SORTPGNUM);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            Integer prev = null;
            System.out.println("\n==============================");
            System.out.println("Ascending order on timestamp\n");
            while(true) {
                try {
                    t = sort.get_next();

                    if(t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();

                    if(prev != null && prev > t.getTimeStamp()) {
                        System.out.println("Unexpected timestamp after sorting!");
                        System.out.println("Expected greater than " + prev + " but got " + t.getTimeStamp());
                        break;
                    }
                    prev = t.getTimeStamp();
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                    break;
                }
            }

            // Sort in ascending order on timestamp
            try {
                fscan = new FileScanMap("test8.in", projlist, expr);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            try {
                sort = new SortMap(null, null, null, fscan, 5, order[1], null, SORTPGNUM);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            prev = null;
            System.out.println("\n==============================");
            System.out.println("Descending order on timestamp\n");
            while(true) {
                try {
                    t = sort.get_next();

                    if(t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();

                    if(prev != null && prev < t.getTimeStamp()) {
                        System.out.println("Unexpected timestamp after sorting!");
                        System.out.println("Expected greater than " + prev + " but got " + t.getTimeStamp());
                        break;
                    }
                    prev = t.getTimeStamp();
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                    break;
                }
            }

            System.err.println("------------------- TEST 6 completed ---------------------\n");
            return status;
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
    }

    public boolean addMaps(int i, Heapfile f, Map m, boolean status) throws IOException, InvalidTupleSizeException {
        Map s = new Map((int) m.getLength());
        byte[] m_data = new byte[(int) m.getLength()];
        s.mapInit(m_data, 0, m.getLength());
        s.setDefaultHdr();
        s.setRowLabel(data2[i]);
        s.setColumnLabel(data2[i]);
        s.setTimeStamp(i);
        s.setValue(Integer.toString(i));
        try {
            f.insertRecordMap(s.returnMapByteArray());
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
        s.setTimeStamp(i + 1);
        try {
            f.insertRecordMap(s.returnMapByteArray());
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
        s.setTimeStamp(i - 1);
        try {
            f.insertRecordMap(s.returnMapByteArray());
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }

        s.setColumnLabel(data2[i + 1]);
        s.setTimeStamp(i);
        try {
            f.insertRecordMap(s.returnMapByteArray());
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
        s.setColumnLabel(data2[i + 1]);
        s.setTimeStamp(i + 1);
        try {
            f.insertRecordMap(s.returnMapByteArray());
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
        s.setColumnLabel(data2[i + 1]);
        s.setTimeStamp(i - 1);
        try {
            f.insertRecordMap(s.returnMapByteArray());
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }

        s.setColumnLabel(data2[i - 1]);
        s.setTimeStamp(i);
        try {
            f.insertRecordMap(s.returnMapByteArray());
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
        s.setColumnLabel(data2[i - 1]);
        s.setTimeStamp(i + 1);
        try {
            f.insertRecordMap(s.returnMapByteArray());
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
        s.setColumnLabel(data2[i - 1]);
        s.setTimeStamp(i - 1);
        try {
            f.insertRecordMap(s.returnMapByteArray());
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
        return status;
    }

    protected boolean test7() {
        System.out.println("------------------------ TEST 7 --------------------------");
        boolean status = OK;
        try {
            MapOrder[] order = new MapOrder[2];
            order[0] = new MapOrder(TupleOrder.Ascending);
            order[1] = new MapOrder(TupleOrder.Descending);

            // create a tuple of appropriate size
            Map m = new Map();
            try {
                m.setDefaultHdr();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            int size = m.size();

            // Create unsorted data file "test1.in"
            MID mid;
            Heapfile f = null;
            try {
                f = new Heapfile("test9.in");
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }


            for (int i = 1; i < NUM_RECORDS - 1; i++) {
                try {
                    if(addMaps(i, f, m, status) == false) {
                        status = FAIL;
                        return status;
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                    return status;
                }
            }

            int rec = f.getRecCntMap();
            System.out.println("Number of records :" + rec);
            System.out.println("Input record number :" + Integer.toString(data2.length - 2));

            // create an iterator by open a file scan
            FldSpec[] projlist = new FldSpec[2];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 0);
            projlist[1] = new FldSpec(rel, 1);

            // set up an identity selection
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopGE);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].operand2.string = "andyw";
            expr[0].next = null;
            expr[1] = new CondExpr();
            expr[1].op = new AttrOperator(AttrOperator.aopLE);
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[1].operand2.string = "randal";
            expr[1].next = null;
            expr[2] = null;

            // Sort in ascending order on row label
            System.out.println("\n==============================");
            System.out.println("Ascending order on row label and then column label\n");
            FileScanMap fscan = null;

            try {
                fscan = new FileScanMap("test9.in", projlist, null);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            // Sort "test8.in"
            SortMap sort = null;
            try {
                sort = new SortMap(null, null, null, fscan, 1, order[0], null, SORTPGNUM);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            Map t = null;

            int i = 1;
            while (true) {
                try {
                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String RowLabel1 = t.getRowLabel();
                    String ColumnLabel1 = t.getColumnLabel();
                    Integer Timestamp1 = t.getTimeStamp();

                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String RowLabel2 = t.getRowLabel();
                    String ColumnLabel2 = t.getColumnLabel();
                    Integer Timestamp2 = t.getTimeStamp();

                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String RowLabel3 = t.getRowLabel();
                    String ColumnLabel3 = t.getColumnLabel();
                    Integer Timestamp3 = t.getTimeStamp();

                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String RowLabel4 = t.getRowLabel();
                    String ColumnLabel4 = t.getColumnLabel();

                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String RowLabel5 = t.getRowLabel();
                    String ColumnLabel5 = t.getColumnLabel();

                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String RowLabel6 = t.getRowLabel();
                    String ColumnLabel6 = t.getColumnLabel();

                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String RowLabel7 = t.getRowLabel();
                    String ColumnLabel7 = t.getColumnLabel();

                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String RowLabel8 = t.getRowLabel();
                    String ColumnLabel8 = t.getColumnLabel();

                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String RowLabel9 = t.getRowLabel();
                    String ColumnLabel9 = t.getColumnLabel();


                    if (!(RowLabel1.equals(data2[i]) && RowLabel2.equals(data2[i])
                            && RowLabel3.equals(data2[i]) && RowLabel4.equals(data2[i])
                            && RowLabel5.equals(data2[i]) && RowLabel6.equals(data2[i])
                            && RowLabel7.equals(data2[i]) && RowLabel7.equals(data2[i])
                            && RowLabel9.equals(data2[i]))) {
                        System.out.println("Unexpected row value after sorting!");
                        System.out.println("Expected :" + data2[i] + " but got " + RowLabel1 + "," + RowLabel2 + ","
                                + RowLabel3 + "," + RowLabel4 + "," + RowLabel5 + "," + RowLabel6 + "," + RowLabel7 + ","
                                + RowLabel8 + "," + RowLabel8);
                        break;
                    }
                    if (!(ColumnLabel1.compareTo(ColumnLabel4) < 0 &&
                            ColumnLabel4.compareTo(ColumnLabel7) < 0)) {
                        System.out.println("Columns are not sorted");
                        System.out.println("Got " + ColumnLabel1 + " for first column label and "
                                + ColumnLabel4 + " for fourth column label and " + ColumnLabel7 + " for seventh column label.");
                        break;
                    }
                    if (!(Timestamp1.compareTo(Timestamp2) < 0 &&
                            Timestamp2.compareTo(Timestamp3) < 0)) {
                        System.out.println("Timestamps are not sorted");
                        System.out.println("Got " + Timestamp1 + " for first timestamp and "
                                + Timestamp2 + " for second timestamp and " + Timestamp3 + " for third timestamp.");
                        break;
                    }
                    i++;
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                    break;
                }
            }

            System.err.println("------------------- TEST 7 completed ---------------------\n");

            return status;
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
    }

    protected boolean test8() {
        System.out.println("------------------------ TEST 8 --------------------------");
        boolean status = OK;
        try {
            MapOrder[] order = new MapOrder[2];
            order[0] = new MapOrder(TupleOrder.Ascending);
            order[1] = new MapOrder(TupleOrder.Descending);

            // create a tuple of appropriate size
            Map m = new Map();
            try {
                m.setDefaultHdr();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            int size = m.size();

            // Create unsorted data file "test1.in"
            MID mid;
            Heapfile f = null;
            try {
                f = new Heapfile("test10.in");
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }


            for (int i = 1; i < NUM_RECORDS-1; i++) {
                Map s = new Map((int) m.getLength());
                try {
                    byte[] m_data = new byte[(int) m.getLength()];
                    s.mapInit(m_data, 0, m.getLength());
                    s.setDefaultHdr();
                    s.setRowLabel(data2[i]);
                    s.setColumnLabel(data2[i]);
                    s.setTimeStamp(i);
                    s.setValue(Integer.toString(i));
                    try {
                        mid = f.insertRecordMap(s.returnMapByteArray());
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        return status;
                    }
                    s.setRowLabel(data2[i + 1]);
                    try {
                        mid = f.insertRecordMap(s.returnMapByteArray());
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        return status;
                    }
                    s.setRowLabel(data2[i - 1]);
                    try {
                        mid = f.insertRecordMap(s.returnMapByteArray());
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        return status;
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }
            int rec = f.getRecCntMap();
            System.out.println("Number of records :" + rec);
            System.out.println("Input record number :" + Integer.toString(data2.length - 2));

            // create an iterator by open a file scan
            FldSpec[] projlist = new FldSpec[2];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 0);
            projlist[1] = new FldSpec(rel, 1);

            // set up an identity selection
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopGE);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].operand2.string = "andyw";
            expr[0].next = null;
            expr[1] = new CondExpr();
            expr[1].op = new AttrOperator(AttrOperator.aopLE);
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[1].operand2.string = "randal";
            expr[1].next = null;
            expr[2] = null;

            // Sort in ascending order on row label
            System.out.println("\n==============================");
            System.out.println("Ascending order on column label and then row label\n");
            FileScanMap fscan = null;

            try {
                fscan = new FileScanMap("test10.in", projlist, null);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            // Sort "test8.in"
            SortMap sort = null;
            try {
                sort = new SortMap(null, null, null, fscan, 2, order[0], null, SORTPGNUM);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            Map t = null;

            int i = 1;
            while (true) {
                try {
                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String firstRowLabel = t.getRowLabel();
                    String firstColumnLabel = t.getColumnLabel();

                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String secondRowLabel = t.getRowLabel();
                    String secondColumnLabel = t.getColumnLabel();

                    t = sort.get_next();
                    if (t == null) {
                        break;
                    }
                    t.setFldOffset(t.getMapByteArray());
                    t.print();
                    String thirdRowLabel = t.getRowLabel();
                    String thirdColumnLabel = t.getColumnLabel();

                    if (!(firstColumnLabel.equals(data2[i]) && secondColumnLabel.equals(data2[i])
                            && thirdColumnLabel.equals(data2[i]))) {
                        System.out.println("Unexpected column value after sorting!");
                        System.out.println("Expected :" + data2[i] + " but got " + firstColumnLabel + " for first column label and "
                                + secondColumnLabel + " for second column label and " + thirdColumnLabel + " for third column label.");
                        break;
                    }
                    if (!(firstRowLabel.compareTo(secondRowLabel) < 0 &&
                            secondRowLabel.compareTo(thirdRowLabel) < 0)) {
                        System.out.println("Rows are not sorted");
                        System.out.println("Got " + firstRowLabel + " for first row label and "
                                + secondRowLabel + " for second row label and " + thirdRowLabel + " for third row label.");
                        break;
                    }
                    i++;
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                    break;
                }
            }

            System.err.println("------------------- TEST 8 completed ---------------------\n");

            return status;
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            return status;
        }
    }

    protected String testName() {
        return "Sort";
    }
}

public class SortTest {
    public static void main(String argv[]) {
        boolean sortstatus;

        SORTDriver sortt = new SORTDriver();

        sortstatus = sortt.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during sorting tests");
        } else {
            System.out.println("Sorting tests completed successfully");
        }
    }
}

