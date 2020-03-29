package BigT;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import BigT.*;
import btree.DeleteFashionException;
import btree.FreePageException;
import btree.IndexFullDeleteException;
import btree.InsertRecException;
import btree.LeafRedistributeException;
import btree.RecordNotFoundException;
import btree.RedistributeException;
import global.*;

/**
 * BatchInsert
 */
public class BatchInsert {
    String datafile;
    String bigTable;
    int storageType;
    bigt table;

    public BatchInsert(bigt table, String datafile, int type, String bigTable) {
        this.table = table;
        this.datafile = datafile;
        this.storageType = type;
        this.bigTable = bigTable;
    }

    public int run() throws DeleteFashionException, LeafRedistributeException, RedistributeException,
            InsertRecException, FreePageException, RecordNotFoundException, IndexFullDeleteException, Exception {
        File inputFile = new File(this.datafile);
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        Map map = new Map();
        String line = "";
        String[] labels;
        int i =1;
        int pages =0;
        long startTime = System.nanoTime();
        while((line=br.readLine())!=null) {
            line = line.replaceAll("[^\\x00-\\x7F]", "");
            i++;
            if(i%1000 == 0){

                System.out.print("*");
            }
            labels = line.split(",");
            map.setDefaultHdr();
            map.setRowLabel(labels[0]);
            map.setColumnLabel(labels[1]);
            map.setTimeStamp(Integer.parseInt(labels[3]));
            String valueLabel = labels[2];
            for(int j=labels[2].length(); j < Map.DEFAULT_STRING_ATTRIBUTE_SIZE; j++){
                valueLabel = "0"+valueLabel;
            }
            map.setValue(valueLabel);
//            System.out.print(i + " -> ");
//            map.print();
            MID mid = table.insertMap(map, storageType);
            pages = mid.pageNo.pid;
        }
        long endTime = System.nanoTime();

        System.out.println();

        System.out.println("TIME TAKEN FOR INSERTING ALL RECORDS "+((endTime - startTime)/1000000000) + " s");

        /*startTime = System.nanoTime();
        table.buildUtilityIndex();
        endTime = System.nanoTime();
        System.out.println("TIME TAKEN TO BUILD UTILITY INDEX " + ((endTime - startTime)/1000000000) + " s");


        startTime = System.nanoTime();
        table.deleteDuplicateRecords();
        endTime = System.nanoTime();
        System.out.println("TIME TAKEN FOR DUPLICATE RECORDS REMOVAL "+((endTime - startTime)/1000000000) + " s");


        startTime = System.nanoTime();
        table.insertIntoMainIndex();
        endTime = System.nanoTime();
        System.out.println("TIME TAKEN FOR CREATING ORIGINAL INDEX "+((endTime - startTime)/1000000000) + " s");*/

        System.out.println();
        return pages;
    }
}
