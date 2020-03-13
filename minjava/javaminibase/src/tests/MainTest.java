package tests;

import java.io.*;

import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;
import btree.*;
import BigT.*;

import java.util.Random;
import java.util.Scanner;

class MainTest implements GlobalConst {

    public static void display(){
        SystemDefs.JavabaseDB.pcounter.initialize();
        System.out.println("------------------------ BigTable Tests --------------------------");
        System.out.println("Press 1 for Batch Insert");
        System.out.println("Press 2 for Query");
        System.out.println("Press 3 to quit");
        System.out.println("------------------------ BigTable Tests --------------------------");
    }

    public static void main(String argv[]) {
        //original code

        String dbpath = "/tmp/maintest" + System.getProperty("user.name") + ".minibase-db";
        String logpath = "/tmp/maintest" + System.getProperty("user.name") + ".minibase-log";
        // SystemDefs.JavabaseDB.pcounter.initialize();

        // // Kill anything that might be hanging around
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

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

        SystemDefs sysdef = new SystemDefs(dbpath, 10000, 1000, "LRU");

        // remove_logcmd = remove_cmd + newlogpath;
        // remove_dbcmd = remove_cmd + newdbpath;

        // //This step seems redundant for me.  But it's in the original
        // //C++ code.  So I am keeping it as of now, just in case I
        // //I missed something
        // try {
        //     Runtime.getRuntime().exec(remove_logcmd);
        //     Runtime.getRuntime().exec(remove_dbcmd);
        // } catch (IOException e) {
        //     System.err.println("" + e);
        // }
        display();
        Scanner sc = new Scanner(System.in);
        String option = sc.nextLine();
        bigt big = null;
        while(option.equals("1")||option.equals("2")||option.equals("4")){
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
                try{
                    File f = new File(dbpath);
                    if(f.exists()) {
                        SystemDefs.MINIBASE_RESTART_FLAG = true;
                    } else {
                        SystemDefs.MINIBASE_RESTART_FLAG = false;
                    }
                    sysdef = new SystemDefs(dbpath, 10000, Integer.parseInt(splits[4]), "LRU");
                    big = new bigt(splits[3], Integer.parseInt(splits[2]));
                    BatchInsert batchInsert = new BatchInsert(big, splits[1]);
                    batchInsert.run();
                    SystemDefs.JavabaseBM.flushAllPages();
                    SystemDefs.JavabaseDB.closeDB();
                }
                catch(Exception e){
                    System.out.println("Error Occured");
                    e.printStackTrace();
                    display();
                    option = sc.nextLine();
                    continue;
                }
            }else if (option.equals("2")){
                System.out.println("FORMAT: query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
                String query = sc.nextLine();
                String[] splits = query.split(" ");
                if(splits.length!=8){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }
                try{
                    File f = new File(dbpath);
                    if(f.exists()) {
                        SystemDefs.MINIBASE_RESTART_FLAG = true;
                    } else {
                        SystemDefs.MINIBASE_RESTART_FLAG = false;
                    }
                    sysdef = new SystemDefs(dbpath, 10000, Integer.parseInt(splits[7]), "LRU");
                    Stream stream = big.openStream(Integer.parseInt(splits[3]), splits[4],
                            splits[5], splits[6], Integer.parseInt(splits[7]));
                    Map t = stream.getNext();
                    while(true) {
                        if (t == null) {
                            break;
                        }
                        t.setFldOffset(t.getMapByteArray());
                        t.print();
                        t = stream.getNext();
                    }
                    System.out.println("Number of unpinned Buffers " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
                    System.out.println("Number of buffers " + SystemDefs.JavabaseBM.getNumBuffers());
                    stream.closestream();
                    SystemDefs.JavabaseBM.flushAllPages();
                    SystemDefs.JavabaseDB.closeDB();
                }
                catch(Exception e){
                    System.out.println("Error Occured");
                    e.printStackTrace();
                    display();
                    option = sc.nextLine();
                    continue;
                }

            }else{
                try{
                    File f = new File(dbpath);
                    if(f.exists()) {
                        SystemDefs.MINIBASE_RESTART_FLAG = true;
                    } else {
                        SystemDefs.MINIBASE_RESTART_FLAG = false;
                    }
                    sysdef = new SystemDefs(dbpath, 10000, 100, "LRU");
                    Scan scan = new Scan(big.getheapfile(), true);
                    RID rid = new RID();
                    Map temp = scan.getNextMap(rid);
                    temp.setFldOffset(temp.getMapByteArray());
                    temp.print();
                    while (temp != null) {
                        temp = scan.getNextMap(rid);
                        if(temp!=null){
                            temp.setFldOffset(temp.getMapByteArray());
                            temp.print();
                        }
                    }
                    System.out.println(big.getMapCnt());
                    SystemDefs.JavabaseBM.flushAllPages();
                    SystemDefs.JavabaseDB.closeDB();
                }
                catch(Exception e){
                    System.out.println("Error Occured");
                    e.printStackTrace();
                    display();
                    option = sc.nextLine();
                    continue;
                }
            }
            System.out.println("READ COUNT : "+SystemDefs.JavabaseDB.pcounter.getRCounter());
            System.out.println("WRITE COUNT : "+SystemDefs.JavabaseDB.pcounter.getWCounter());
            display();
            option = sc.nextLine();
        }

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
