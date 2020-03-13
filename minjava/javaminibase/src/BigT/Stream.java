package BigT;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.MapOrder;
import index.MapIndexScan;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Stream {

    int indexType;
    int orderType;
    int numBuf;
    MapIterator mapIterator;
    SortMap sortMap;

    CondExpr[] condExprs;

    public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {
        this.indexType = bigtable.getType();
        this.orderType = orderType;
        this.numBuf = numBuf;
        List<CondExpr> exprs = new ArrayList<CondExpr>();
        exprs.addAll(processFilter(rowFilter, 1));
        exprs.addAll(processFilter(columnFilter, 2));
        exprs.addAll(processFilter(valueFilter, 4));
        condExprs = new CondExpr[exprs.size() + 1];
        int i = 0;
        for (CondExpr expr : exprs) {
            condExprs[i++] = expr;
        }
        condExprs[i] = null;
        try {
            switch (indexType) {
                case 1:
                    mapIterator = new FileScanMap(bigtable.getName(), null, condExprs);
                    break;
                default:
                    AttrType[] attrType = new AttrType[4];
                    attrType[0] = new AttrType(AttrType.attrString);
                    attrType[1] = new AttrType(AttrType.attrString);
                    attrType[2] = new AttrType(AttrType.attrInteger);
                    attrType[3] = new AttrType(AttrType.attrString);
                    short[] res_str_sizes = new short[]{Map.DEFAULT_STRING_ATTRIBUTE_SIZE,
                            Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};
                    mapIterator = new MapIndexScan(new IndexType(IndexType.B_Index), bigtable.getName(), "index1",
                            attrType, res_str_sizes, 4, 4, null, condExprs, indexType, false);
            }
            sortMap = new SortMap(null, null, null, mapIterator, this.orderType, new MapOrder(MapOrder.Ascending), null, this.numBuf);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred while initiating the stream");
        }
    }

    private List<CondExpr> processFilter(String filter, int fldNum) {
        List<CondExpr> result = new ArrayList<CondExpr>();
        if (filter.equals("*")) {

        } else if (filter.contains("[")) {
            String[] filterSplit = filter.substring(1, filter.length() - 1).split(",");
            CondExpr expr1 = new CondExpr();
            expr1.op = new AttrOperator(AttrOperator.aopGE);
            expr1.type1 = new AttrType(AttrType.attrSymbol);
            expr1.type2 = new AttrType(AttrType.attrString);
            expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
            expr1.operand2.string = filterSplit[0];
            expr1.next = null;
            CondExpr expr2 = new CondExpr();
            expr2 = new CondExpr();
            expr2.op = new AttrOperator(AttrOperator.aopLE);
            expr2.type1 = new AttrType(AttrType.attrSymbol);
            expr2.type2 = new AttrType(AttrType.attrString);
            expr2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
            expr2.operand2.string = filterSplit[1];
            expr2.next = null;
            result.add(expr1);
            result.add(expr2);
        } else {
            CondExpr expr = new CondExpr();
            expr.op = new AttrOperator(AttrOperator.aopEQ);
            expr.type1 = new AttrType(AttrType.attrSymbol);
            expr.type2 = new AttrType(AttrType.attrString);
            expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
            expr.operand2.string = filter;
            expr.next = null;
            result.add(expr);
        }
        return result;
    }

    public Map getNext() {
        try {
            return sortMap.get_next();
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred while iterating through stream!");
            return null;
        }
    }

    public void closestream() {
        try {
            mapIterator.close();
            sortMap.close();
        } catch (Exception e){
            e.printStackTrace();
            System.out.println("Exception occurred while closing the stream!");
        }
    }
}