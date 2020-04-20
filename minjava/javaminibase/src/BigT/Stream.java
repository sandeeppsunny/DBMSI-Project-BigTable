package BigT;

import global.AttrOperator;
import global.AttrType;
import global.MapOrder;
import iterator.*;

import java.util.ArrayList;
import java.util.List;

public class Stream {

    int indexType;
    int orderType;
    int numBuf;
    MapIterator mapIterator;
    SortMap sortMap;

    CondExpr[] condExprs;
    CondExpr[] index_2_filter;
    CondExpr[] index_3_filter;
    CondExpr[] index_4_filter;
    CondExpr[] index_5_filter;

    public Stream(String bigtName, String indexFilename, int indexType, int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {
        this.indexType = indexType;
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

        index_2_filter = getKeyFilterForIndexType(2, rowFilter, columnFilter, valueFilter);
        index_3_filter = getKeyFilterForIndexType(3, rowFilter, columnFilter, valueFilter);
        index_4_filter = getKeyFilterForIndexType(4, rowFilter, columnFilter, valueFilter);
        index_5_filter = getKeyFilterForIndexType(5, rowFilter, columnFilter, valueFilter);
//        int keyFldNum = 1;
//        switch(indexType) {
//            case 2:
//                keyFldNum = 1;
//                break;
//            case 3:
//                keyFldNum = 2;
//                break;
//            case 4:
//                keyFldNum = 2;
//                break;
//            case 5:
//                keyFldNum = 1;
//                break;
//        }

        try {
            switch (indexType) {
                case 1:
                    mapIterator = new FileScanMap(bigtName, null, condExprs, true);
                    break;
                default:
//                    AttrType[] attrType = new AttrType[4];
//                    attrType[0] = new AttrType(AttrType.attrString);
//                    attrType[1] = new AttrType(AttrType.attrString);
//                    attrType[2] = new AttrType(AttrType.attrInteger);
//                    attrType[3] = new AttrType(AttrType.attrString);
//                    short[] res_str_sizes = new short[]{Map.DEFAULT_STRING_ATTRIBUTE_SIZE,
//                            Map.DEFAULT_STRING_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE};
//                    boolean flag = true;
//                    if(indexType == 2){
//                        if(rowFilter.equals("*")){
//                            flag = false;
//                        }
//                    }else if(indexType == 3){
//                        if(columnFilter.equals("*")){
//                            flag = false;
//                        }
//                    }else if(indexType == 4){
//                        if(columnFilter.equals("*")){
//                            flag = false;
//                        }
//                    }else if(indexType == 5){
//                        if(rowFilter.equals("*")){
//                            flag = false;
//                        }
//                    }

//                    if(flag){
//                        mapIterator = new MapIndexScan(new IndexType(IndexType.B_Index), bigtName, indexFilename,
//                                attrType, res_str_sizes, 4, 4, null, condExprs, condExprForKey, keyFldNum, false);
//                    }else {
                    mapIterator = new CombinedScanMap(bigtName, condExprs, index_2_filter, index_3_filter, index_4_filter, index_5_filter);
//                    }
                    break;
            }
            sortMap = new SortMap(null, null, null, mapIterator, this.orderType, new MapOrder(MapOrder.Ascending), null, this.numBuf);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred while initiating the stream");
        }
    }

    public static CondExpr[] getKeyFilterForIndexType(int indexType, String rowFilter, String columnFilter, String valueFilter) {
        List<CondExpr> exprForKey = new ArrayList<CondExpr>();
        switch(indexType) {
            case 2:
                exprForKey.addAll(processFilter(rowFilter, 1));
                break;
            case 3:
                exprForKey.addAll(processFilter(columnFilter, 2));
                break;
            case 4:
                List<CondExpr> columnKeyFilter = processFilter(columnFilter, 2);
                if(columnKeyFilter.isEmpty()) {
                    break;
                }
                List<CondExpr> rowKeyFilter = processFilter(rowFilter, 1);
                if(rowKeyFilter.isEmpty()) {
                    exprForKey.addAll(columnKeyFilter);
                    break;
                }
                int columnFilterSize = columnKeyFilter.size();
                int rowFilterSize = rowKeyFilter.size();
                if(columnFilterSize == 1 && rowFilterSize == 1) {
                    CondExpr expr = new CondExpr();
                    expr.op = new AttrOperator(AttrOperator.aopEQ);
                    expr.type1 = new AttrType(AttrType.attrSymbol);
                    expr.type2 = new AttrType(AttrType.attrString);
                    expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expr.operand2.string = columnKeyFilter.get(0).operand2.string + "%" + rowKeyFilter.get(0).operand2.string;
                    expr.next = null;
                    exprForKey.add(expr);
                    break;
                } else if(columnFilterSize == 1 && rowFilterSize == 2) {
                    CondExpr expr1 = new CondExpr();
                    expr1.op = new AttrOperator(AttrOperator.aopGE);
                    expr1.type1 = new AttrType(AttrType.attrSymbol);
                    expr1.type2 = new AttrType(AttrType.attrString);
                    expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expr1.operand2.string = columnKeyFilter.get(0).operand2.string + "%" + rowKeyFilter.get(0).operand2.string;;
                    expr1.next = null;
                    CondExpr expr2 = new CondExpr();
                    expr2.op = new AttrOperator(AttrOperator.aopLE);
                    expr2.type1 = new AttrType(AttrType.attrSymbol);
                    expr2.type2 = new AttrType(AttrType.attrString);
                    expr2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expr2.operand2.string = columnKeyFilter.get(0).operand2.string + "%" + rowKeyFilter.get(1).operand2.string;;
                    expr2.next = null;
                    exprForKey.add(expr1);
                    exprForKey.add(expr2);
                } else if(columnFilterSize == 2 && rowFilterSize == 1) {
                    CondExpr expr1 = new CondExpr();
                    expr1.op = new AttrOperator(AttrOperator.aopGE);
                    expr1.type1 = new AttrType(AttrType.attrSymbol);
                    expr1.type2 = new AttrType(AttrType.attrString);
                    expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expr1.operand2.string = columnKeyFilter.get(0).operand2.string + "%" + rowKeyFilter.get(0).operand2.string;;
                    expr1.next = null;
                    CondExpr expr2 = new CondExpr();
                    expr2.op = new AttrOperator(AttrOperator.aopLE);
                    expr2.type1 = new AttrType(AttrType.attrSymbol);
                    expr2.type2 = new AttrType(AttrType.attrString);
                    expr2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expr2.operand2.string = columnKeyFilter.get(1).operand2.string + "%" + rowKeyFilter.get(0).operand2.string;;
                    expr2.next = null;
                    exprForKey.add(expr1);
                    exprForKey.add(expr2);
                } else if(columnFilterSize == 2 && rowFilterSize == 2) {
                    CondExpr expr1 = new CondExpr();
                    expr1.op = new AttrOperator(AttrOperator.aopGE);
                    expr1.type1 = new AttrType(AttrType.attrSymbol);
                    expr1.type2 = new AttrType(AttrType.attrString);
                    expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expr1.operand2.string = columnKeyFilter.get(0).operand2.string + "%" + rowKeyFilter.get(0).operand2.string;;
                    expr1.next = null;
                    CondExpr expr2 = new CondExpr();
                    expr2.op = new AttrOperator(AttrOperator.aopLE);
                    expr2.type1 = new AttrType(AttrType.attrSymbol);
                    expr2.type2 = new AttrType(AttrType.attrString);
                    expr2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
                    expr2.operand2.string = columnKeyFilter.get(1).operand2.string + "%" + rowKeyFilter.get(1).operand2.string;;
                    expr2.next = null;
                    exprForKey.add(expr1);
                    exprForKey.add(expr2);
                }
                break;
            case 5:
                List<CondExpr> rowKeyFilter_2 = processFilter(rowFilter, 1);
                if(rowKeyFilter_2.isEmpty()) {
                    break;
                }
                List<CondExpr> valueKeyFilter = processFilter(valueFilter, 4);
                if(valueKeyFilter.isEmpty()) {
                    exprForKey.addAll(rowKeyFilter_2);
                    break;
                }
                int rowKeyFilter_2Size = rowKeyFilter_2.size();
                int valueKeyFilterSize = valueKeyFilter.size();
                if(rowKeyFilter_2Size == 1 && valueKeyFilterSize == 1) {
                    CondExpr expr = new CondExpr();
                    expr.op = new AttrOperator(AttrOperator.aopEQ);
                    expr.type1 = new AttrType(AttrType.attrSymbol);
                    expr.type2 = new AttrType(AttrType.attrString);
                    expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expr.operand2.string = rowKeyFilter_2.get(0).operand2.string + "%" + padValue(valueKeyFilter.get(0).operand2.string);
                    expr.next = null;
                    exprForKey.add(expr);
                    break;
                } else if(rowKeyFilter_2Size == 1 && valueKeyFilterSize == 2) {
                    CondExpr expr1 = new CondExpr();
                    expr1.op = new AttrOperator(AttrOperator.aopGE);
                    expr1.type1 = new AttrType(AttrType.attrSymbol);
                    expr1.type2 = new AttrType(AttrType.attrString);
                    expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expr1.operand2.string = rowKeyFilter_2.get(0).operand2.string + "%" + padValue(valueKeyFilter.get(0).operand2.string);
                    expr1.next = null;
                    CondExpr expr2 = new CondExpr();
                    expr2.op = new AttrOperator(AttrOperator.aopLE);
                    expr2.type1 = new AttrType(AttrType.attrSymbol);
                    expr2.type2 = new AttrType(AttrType.attrString);
                    expr2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expr2.operand2.string = rowKeyFilter_2.get(0).operand2.string + "%" + padValue(valueKeyFilter.get(1).operand2.string);
                    expr2.next = null;
                    exprForKey.add(expr1);
                    exprForKey.add(expr2);
                } else if(rowKeyFilter_2Size == 2 && valueKeyFilterSize == 1) {
                    CondExpr expr1 = new CondExpr();
                    expr1.op = new AttrOperator(AttrOperator.aopGE);
                    expr1.type1 = new AttrType(AttrType.attrSymbol);
                    expr1.type2 = new AttrType(AttrType.attrString);
                    expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expr1.operand2.string = rowKeyFilter_2.get(0).operand2.string + "%" + padValue(valueKeyFilter.get(0).operand2.string);
                    expr1.next = null;
                    CondExpr expr2 = new CondExpr();
                    expr2.op = new AttrOperator(AttrOperator.aopLE);
                    expr2.type1 = new AttrType(AttrType.attrSymbol);
                    expr2.type2 = new AttrType(AttrType.attrString);
                    expr2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expr2.operand2.string = rowKeyFilter_2.get(1).operand2.string + "%" + padValue(valueKeyFilter.get(0).operand2.string);
                    expr2.next = null;
                    exprForKey.add(expr1);
                    exprForKey.add(expr2);
                } else if(rowKeyFilter_2Size == 2 && valueKeyFilterSize == 2) {
                    CondExpr expr1 = new CondExpr();
                    expr1.op = new AttrOperator(AttrOperator.aopGE);
                    expr1.type1 = new AttrType(AttrType.attrSymbol);
                    expr1.type2 = new AttrType(AttrType.attrString);
                    expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expr1.operand2.string = rowKeyFilter_2.get(0).operand2.string + "%" + padValue(valueKeyFilter.get(0).operand2.string);
                    expr1.next = null;
                    CondExpr expr2 = new CondExpr();
                    expr2.op = new AttrOperator(AttrOperator.aopLE);
                    expr2.type1 = new AttrType(AttrType.attrSymbol);
                    expr2.type2 = new AttrType(AttrType.attrString);
                    expr2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                    expr2.operand2.string = rowKeyFilter_2.get(1).operand2.string + "%" + padValue(valueKeyFilter.get(1).operand2.string);
                    expr2.next = null;
                    exprForKey.add(expr1);
                    exprForKey.add(expr2);
                }
                break;
        }
        CondExpr[] result = new CondExpr[exprForKey.size() + 1];
        int i = 0;
        for (CondExpr expr : exprForKey) {
            result[i++] = expr;
        }
        result[i] = null;
        return result;
    }

    private static List<CondExpr> processFilter(String filter, int fldNum) {
        List<CondExpr> result = new ArrayList<CondExpr>();
        if (filter.equals("*")) {

        } else if (filter.contains("[")) {
            String[] filterSplit = filter.substring(1, filter.length() - 1).split(",");
            CondExpr expr1 = new CondExpr();
            expr1.op = new AttrOperator(AttrOperator.aopGE);
            expr1.type1 = new AttrType(AttrType.attrSymbol);
            expr1.type2 = new AttrType(AttrType.attrString);
            expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
            if(fldNum == 4){
                expr1.operand2.string = padValue(filterSplit[0]);
            }else{
                expr1.operand2.string = filterSplit[0];
            }
            expr1.next = null;
            CondExpr expr2 = new CondExpr();
            expr2.op = new AttrOperator(AttrOperator.aopLE);
            expr2.type1 = new AttrType(AttrType.attrSymbol);
            expr2.type2 = new AttrType(AttrType.attrString);
            expr2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
            if(fldNum == 4){
                expr2.operand2.string = padValue(filterSplit[1]);
            }else{
                expr2.operand2.string = filterSplit[1];
            }
            expr2.next = null;
            result.add(expr1);
            result.add(expr2);
        } else {
            CondExpr expr = new CondExpr();
            expr.op = new AttrOperator(AttrOperator.aopEQ);
            expr.type1 = new AttrType(AttrType.attrSymbol);
            expr.type2 = new AttrType(AttrType.attrString);
            expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
            if(fldNum == 4){
                expr.operand2.string = padValue(filter);
            }else{
                expr.operand2.string = filter;
            }
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

    public static String padValue(String valueFilterString){
        int length = valueFilterString.length();
        for(int i = length; i < Map.DEFAULT_STRING_ATTRIBUTE_SIZE; i++){
            valueFilterString = "0"+valueFilterString;
        }
        return valueFilterString;
    }

    public void closestream() {
        try {
            sortMap.close();
        } catch (Exception e){
            e.printStackTrace();
            System.out.println("Exception occurred while closing the stream!");
        }
    }
}