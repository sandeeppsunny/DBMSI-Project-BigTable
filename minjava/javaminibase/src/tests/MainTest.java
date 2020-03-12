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
        SystemDefs sysdef = new SystemDefs(dbpath, 300, NUMBUF, "Clock");

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
        display();
        Scanner sc = new Scanner(System.in);
        String option = sc.nextLine();
        bigt big = null;
        while(option.equals("1")||option.equals("2")){
            if(option.equals("1")){
                System.out.println("FORMAT: batchinsert DATAFILENAME TYPE BIGTABLENAME");
                String batch = sc.nextLine();
                String[] splits = batch.split(" ");
                if(splits.length!=4){
                    System.out.println("Wrong format, try again!");
                    display();
                    option = sc.nextLine();
                    continue;
                }
                try{
                    big = new bigt(splits[3], Integer.parseInt(splits[2]));
                    BatchInsert batchInsert = new BatchInsert(big, splits[1], Integer.parseInt(splits[2]), splits[3]);
                    batchInsert.run();

                }
                catch(Exception e){
                    System.out.println("Error Occured");
                    display();
                    option = sc.nextLine();
                    continue;
                }
            }else{
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
                    bigt bigTable = new bigt(splits[1], Integer.parseInt(splits[2]));
                }
                catch(Exception e){
                    System.out.println("Error Occured");
                    display();
                    option = sc.nextLine();
                    continue;
                }

            }
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
