package BigT;

import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import index.IndexException;
import index.MapIndexScan;
import index.UnknownIndexTypeException;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RowSort{

    private bigt tempTable;
    private bigt resultTable;
    private int numBuf;
    private bigt sourceTable;
    private String sourceTableName;
    private String ColumnName;
    private String tempTableName;

    public RowSort(String sourceTableName, String resultTable, String ColumnName, int n_pages)
        throws Exception{
        this.numBuf = n_pages;
        this.sourceTableName = sourceTableName;
        this.ColumnName = ColumnName;
        this.tempTableName = "tempTable";
        this.sourceTable = new bigt(sourceTableName, false);
        this.resultTable = new bigt(resultTable, false);
        this.tempTable = new bigt(tempTableName, false);
    }

    public void run() throws Exception{
        FileScanMap mapIterator = new FileScanMap(sourceTableName, null, null, true);
        SortMap sortMap = new SortMap(null, null, null, mapIterator, 1, new MapOrder(MapOrder.Ascending), null, numBuf);
        Map t = sortMap.get_next();
        String row_label = "";
        boolean found = false;
        Map foundMap = new Map();
        String defaultLabel = "";
        while(t != null) {
            t.setFldOffset(t.getMapByteArray());
            if(!row_label.equals(t.getRowLabel())){
                defaultLabel = row_label;
                if(!found && !row_label.equals("")){
                    insertDefault(defaultLabel);
                }
                if(found){
                    if(!row_label.equals(t.getRowLabel())){
                        insertTemp(foundMap);
                        found = false;
                    }
                }
                row_label = t.getRowLabel();
            }
            if(ColumnName.equals(t.getColumnLabel())){
                found = true;
                foundMap = new Map(t);
            }
            t = sortMap.get_next();
        }
        if(found){
            insertTemp(foundMap);
        }else{
            insertDefault(row_label);
        }
        sortMap.close();

        FileScanMap mapIterator1 = new FileScanMap(tempTableName, null, null, true);
        SortMap sortMap1 = new SortMap(null, null, null, mapIterator1, 7, new MapOrder(MapOrder.Ascending), null, numBuf);
        Map t1 = sortMap1.get_next();
        while(t1!=null){
            t1.setFldOffset(t1.getMapByteArray());
            insertDefault(t1.getRowLabel());
            t1 = sortMap1.get_next();
        }
        sortMap1.close();
    }

    public void insertDefault(String rowLabel)
            throws Exception{
        CondExpr[] expr = getConditionalExpression(rowLabel);
        FileScanMap search = new FileScanMap(sourceTableName, null, expr, true);
        Pair pair = search.get_next_mid();
        while(pair != null) {
            resultTable.insertMap(pair.getMap(), 1);
            pair = search.get_next_mid();
        }
        search.close();
    }

    public void insertTemp(Map map)
            throws Exception{
        tempTable.insertMap(map, 1);
    }

    public CondExpr[] getConditionalExpression(String rowLabel){
        CondExpr[] res = new CondExpr[2];
        CondExpr expr = new CondExpr();
        expr.op = new AttrOperator(AttrOperator.aopEQ);
        expr.type1 = new AttrType(AttrType.attrSymbol);
        expr.type2 = new AttrType(AttrType.attrString);
        expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        expr.operand2.string = rowLabel;
        expr.next = null;
        res[0] = expr;
        res[1] = null;

        return res;
    }
}
