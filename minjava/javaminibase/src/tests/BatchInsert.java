package tests;

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


/**
 * BatchInsert
 */
public class BatchInsert {
    String datafile;
    String bigTable;
    int type;
    bigt table;

    BatchInsert(bigt table, String datafile, int type, String bigTable) {
        this.table = table;
        this.datafile = datafile;
        this.type = type;
        this.bigTable = bigTable;
    }

    public void run() throws DeleteFashionException, LeafRedistributeException, RedistributeException,
            InsertRecException, FreePageException, RecordNotFoundException, IndexFullDeleteException, Exception {
        List<String> lines = Files.readAllLines(Paths.get(this.datafile));
        Map map = new Map();
        String[] labels;
        for (String line : lines) {
            labels = line.split(",");
            map.setDefaultHdr();
            map.setRowLabel(labels[0]);
            map.setColumnLabel(labels[1]);
            map.setTimeStamp(Integer.parseInt(labels[2]));
            map.setValue(labels[3]);
            table.insertMap(map);
        }
    }
}
