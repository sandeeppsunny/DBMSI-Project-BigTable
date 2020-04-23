package tests;

import java.io.*;

import global.*;
import iterator.*;
import BigT.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;

class MainTest implements GlobalConst {

    public static HashSet<String> allBigT;

    public static void display(){
//        SystemDefs.JavabaseDB.pcounter.initialize();
        System.out.println("------------------------ BigTable Tests --------------------------");
        System.out.println("Press 1 for Batch Insert");
        System.out.println("Press 2 for Query");
        System.out.println("Press 3 for MapInsert");
        System.out.println("Press 4 for RowJoin");
        System.out.println("Press 5 for RowSort");
        System.out.println("Press 6 for getCounts");
        System.out.println("Press 7 for other options");
        System.out.println("Press 8 to quit");
        System.out.println("------------------------ BigTable Tests --------------------------");
    }

    public static void displayOtherOptions(){
        System.out.println("----------------------Other Utility functions----------------------");
        System.out.println("Press 1 for Normal Scan");
        System.out.println("Press 2 for Row label count");
        System.out.println("Press 3 for Column label count");
        System.out.println("Press 4 to quit this mode");
    }

    public static void main(String argv[]) {
        //original code

        String dbpath = "/tmp/maintest" + System.getProperty("user.name") + ".minibase-db";
        String logpath = "/tmp/maintest" + System.getProperty("user.name") + ".minibase-log";
        SystemDefs sysdef = null;

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
        sysdef = new SystemDefs(dbpath, 100000, 1000, "Clock");
        display();
        Scanner sc = new Scanner(System.in);
        String option = sc.nextLine();
        bigt big = null;
        int pages = 0;
        String replacement_policy = "Clock";
        allBigT = new HashSet<>();
        while(!option.equals("8")){
            if(option.equals("1")){
                System.out.println("FORMAT: batchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUF");
                String batch = sc.nextLine();
                String[] splits = batch.split(" ");
                if(splits.length!=5){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }
//                dbpath = "/tmp/" + splits[3] + ".minibase-db";

                SystemDefs.JavabaseDB.pcounter.initialize();
                try{
                    long startTime = System.nanoTime();
                    sysdef.changeNumberOfBuffers(Integer.parseInt(splits[4]), replacement_policy);
                    big = new bigt(splits[3], true);
                    BatchInsert batchInsert = new BatchInsert(big, splits[1], Integer.parseInt(splits[2]), splits[3]);
                    pages = batchInsert.run();
                    long endTime = System.nanoTime();
                    allBigT.add(splits[3]);
                    System.out.println("TIME TAKEN "+((endTime - startTime)/1000000000) + " s");
                }
                catch(Exception e){
                    System.out.println("Error Occured");
                    e.printStackTrace();
                    display();
                    option = sc.nextLine();
                    continue;
                }
            }else if (option.equals("2")){
                System.out.println("FORMAT: query BIGTABLENAME ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
                String query = sc.nextLine();
                String[] splits = query.split(" ");
                if(splits.length!=7){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }
                try{
                    long startTime = System.nanoTime();
                    sysdef.changeNumberOfBuffers(Integer.parseInt(splits[6]), replacement_policy);
                    SystemDefs.JavabaseDB.pcounter.initialize();
                    big = new bigt(splits[1], false);
                    Stream stream = big.openStream(splits[1], Integer.parseInt(splits[2]), splits[3],
                            splits[4], splits[5], (int)((Integer.parseInt(splits[6])*3)/4));
                    int count =0;
                    Map t = stream.getNext();
                    while(true) {
                        if (t == null) {
                            break;
                        }
                        count++;
                        t.setFldOffset(t.getMapByteArray());
                         t.print();
                        t = stream.getNext();
                    }
//                    SystemDefs.JavabaseBM.displayFrameDesc();
//                    big.unpinAllPages();
//                    System.out.println("Number of unpinned Buffers " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
//                    System.out.println("Number of buffers " + SystemDefs.JavabaseBM.getNumBuffers());
                    stream.closestream();
                    long endTime = System.nanoTime();
                    System.out.println("TIME TAKEN "+((endTime - startTime)/1000000000) + " s");
                    System.out.println("RECORD COUNT : "+count);
                }
                catch(Exception e){
                    System.out.println("Error Occured");
                    e.printStackTrace();
                    display();
                    option = sc.nextLine();
                    continue;
                }
            }else if(option.equals("3")){
                System.out.println("FORMAT: mapinsert ROWLABEL COLUMNLABEL VALUE TIMESTAMP TYPE BIGTABLENAME NUMBUF");
                String[] splits = sc.nextLine().split(" ");
                if(splits.length!=8){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }
                try{
                    sysdef.changeNumberOfBuffers(Integer.parseInt(splits[7]), replacement_policy);
                    SystemDefs.JavabaseDB.pcounter.initialize();
                }catch(Exception e){
                    System.err.println("MainTest.java: Exception in setting the NUMBUF");
                    e.printStackTrace();
                }
                try{
                    MapInsert mapinsert = new MapInsert(splits[1], splits[2], splits[3],
                            Integer.parseInt(splits[4]), Integer.parseInt(splits[5]), splits[6]);
                    mapinsert.run();
                }catch(Exception e){
                    System.err.println("MainTest.java: Exception caused in executing MapInsert");
                    e.printStackTrace();
                    display();
                    option = sc.nextLine();
                    continue;
                }
            }else if(option.equals("4")){
                System.out.println("FORMAT: rowjoin BTNAME1 BTNAME2 OUTBTNAME COLUMNFILTER NUMBUF");
                String[] splits = sc.nextLine().split(" ");
                if(splits.length!=6){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }
                try{
                    sysdef.changeNumberOfBuffers(Integer.parseInt(splits[5]), replacement_policy);
                    SystemDefs.JavabaseDB.pcounter.initialize();
                }catch(Exception e){
                    System.err.println("MainTest.java: Exception in setting the NUMBUF");
                    e.printStackTrace();
                }
                long startTime = System.nanoTime();
                try{
                    RowJoin rowJoin = new RowJoin(splits[1], splits[2], splits[3], splits[4]);
                    rowJoin.run();
                    allBigT.add(splits[3]);
                }catch(Exception e){
                    System.err.println("MainTest.java: Exception caused in executing RowJoin");
                    e.printStackTrace();
                    display();
                    option = sc.nextLine();
                    continue;
                }
                long endTime = System.nanoTime();
                System.out.println("TIME TAKEN "+((endTime - startTime)/1000000000) + " s");

            }else if (option.equals("5")){
                System.out.println("FORMAT: rowsort INBTNAME OUTBTNAME ROWORDER COLUMNNAME NUMBUF");
                System.out.println("ROWORDER: \n 1. Ascending\n 2. Descending" );
                String[] splits = sc.nextLine().split(" ");
                if(splits.length!=6){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }
                try{
                    sysdef.changeNumberOfBuffers(Integer.parseInt(splits[5]), replacement_policy);
                    SystemDefs.JavabaseDB.pcounter.initialize();

                }catch(Exception e){
                    System.err.println("MainTest.java: Exception in setting the NUMBUF");
                    e.printStackTrace();
                }
                long startTime = System.nanoTime();
                try{
                    RowSort rowSort = new RowSort(splits[1], splits[2], Integer.parseInt(splits[3]), splits[4], (int)((Integer.parseInt(splits[5])*3)/4));
                    rowSort.run();
                    allBigT.add(splits[2]);
                }catch(Exception e){
                    System.err.println("MainTest.java: Exception caused in executing RowSort");
                    e.printStackTrace();
                    display();
                    option = sc.nextLine();
                    continue;
                }
                long endTime = System.nanoTime();
                System.out.println("TIME TAKEN "+((endTime - startTime)/1000000000) + " s");

            }else if(option.equals("6")){
                System.out.println("FORMAT: getCounts NUMBUF");
                String[] splits = sc.nextLine().split(" ");
                if(splits.length!=2){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }
                try{
                    sysdef.changeNumberOfBuffers(Integer.parseInt(splits[1]), replacement_policy);
                    SystemDefs.JavabaseDB.pcounter.initialize();

                }catch(Exception e){
                    System.err.println("MainTest.java: Exception in setting the NUMBUF");
                    e.printStackTrace();
                }
                long startTime = System.nanoTime();
                try{
                    GetAllCount getAllCount = new GetAllCount(allBigT, Integer.parseInt(splits[1]));
                    getAllCount.run();
                }catch(Exception e){
                    System.err.println("MainTest.java: Exception caused in executing getCounts");
                    e.printStackTrace();
                    display();
                    option = sc.nextLine();
                    continue;
                }
                long endTime = System.nanoTime();
                System.out.println("TIME TAKEN "+((endTime - startTime)/1000000000) + " s");
            }else if (option.equals("7")){
                System.out.println("Enter BigTable name");
                String bigt_name = sc.nextLine();
                displayOtherOptions();
                String otherOption = sc.nextLine();
                while(!otherOption.equals("4")){
                    if(otherOption.equals("1")){
                        try{
                            SystemDefs.JavabaseDB.pcounter.initialize();
                            big = new bigt(bigt_name, false);
                            for(int i = 1; i<= 5; i++){
                                System.out.println("----------------------------");
                                System.out.println("Storage Type " + i);
                                System.out.println("****************************");
                                FileScanMap fscan = new FileScanMap(big.getHeapFileName(i), null, null, false);
                                Map temp = fscan.get_next();
                                while (temp != null) {
                                    temp.setFldOffset(temp.getMapByteArray());
                                    temp.print();
                                    temp = fscan.get_next();
                                }
                                fscan.close();
                            }
                            System.out.println("RECORD COUNT: " + big.getMapCnt());
                        }
                        catch(Exception e){
                            System.out.println("Error Occured");
                            e.printStackTrace();
                            displayOtherOptions();
                            otherOption = sc.nextLine();
                            continue;
                        }
                    }else if(otherOption.equals("2")){
                        try{
                            SystemDefs.JavabaseDB.pcounter.initialize();
                            bigt bigtable = new bigt(bigt_name, false);
                            System.out.println("ROW COUNT: " + bigtable.getRowCnt());
                        }catch(Exception e){
                            System.out.println("Error Occured");
                            e.printStackTrace();
                            displayOtherOptions();
                            otherOption = sc.nextLine();
                            continue;
                        }
                    }else if(otherOption.equals("3")){
                        try{
                            SystemDefs.JavabaseDB.pcounter.initialize();
                            bigt bigtable = new bigt(bigt_name, false);
                            System.out.println("COLUMN COUNT: " + bigtable.getColumnCnt());
                        }catch(Exception e){
                            System.out.println("Error Occured");
                            e.printStackTrace();
                            displayOtherOptions();
                            otherOption = sc.nextLine();
                            continue;
                        }
                    }
                    displayOtherOptions();
                    otherOption = sc.nextLine();
                }
            }
            try {
                int read = SystemDefs.JavabaseDB.pcounter.getRCounter();
                int write = SystemDefs.JavabaseDB.pcounter.getWCounter();
                System.out.println("READ COUNT : "+read);
                System.out.println("WRITE COUNT : "+write);
//                System.out.println("PAGE COUNT : "+pages);
            }catch (Exception e) {
                System.out.println("Wrong Input!");
            }
            display();
            option = sc.nextLine();
        }

        remove_dbcmd = remove_cmd + dbpath;
        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (Exception e) {
            System.err.println("" + e);
        }

        System.out.println("--------------- BigTable Tests Complete --------------------------");

    }

    /*
    -----Wrong Format-----
    System.out.println("Wrong format, try again!");
    display();
    option = sc.nextLine();
    continue;
    -----Error Occured-----
    System.out.println("Error Occured");
    display();
    option = sc.nextLine();
    continue;
    -----------------------
    */


}
