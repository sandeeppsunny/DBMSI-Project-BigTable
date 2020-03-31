package BigT;

import bufmgr.*;
import global.*;
import heap.Heapfile;
import heap.HeapfileInterface;
import heap.Scan;
import heap.SortedHeapfile;
import index.MapIndexScan;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class CombinedStream {
    Stream[] openStreams;
    private Heapfile[] heapFiles;
    private Scan[] scanHeapFiles;
    private Integer smallestScanIndex;
    PriorityQueue<Pair> pq;
    int orderType;

    public CombinedStream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter, int numBuf) {
        openStreams = new Stream[5];
        heapFiles = new Heapfile[5];
        scanHeapFiles = new Scan[5];
        for (int i = 0; i < 5; i++) {
            openStreams[i] = new Stream(bigtable.getHeapFileName(i + 1),
                    bigtable.getIndexFileName(i + 1), 1,
                    orderType, rowFilter, columnFilter, valueFilter, numBuf);
            try {
                heapFiles[i] = new Heapfile(bigtable.getHeapFileName(i + 1) + "_stream");
                boolean isScanComplete = false;
                while (!isScanComplete) {
                    Map map = openStreams[i].getNext();
                    if (map == null) {
                        isScanComplete = true;
                        break;
                    }
                    heapFiles[i].insertRecordMap(map.getMapByteArray());
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Exception occurred while sorting");
            } finally {
                openStreams[i].closestream();
            }
        }
        this.orderType = orderType;
        // Only 5 pairs will be in the priority queue at any time
        pq = new PriorityQueue<Pair>(new Comparator<Pair>() {
            @Override
            public int compare(Pair p1, Pair p2) {
                Map o1 = p1.getMap();
                Map o2 = p2.getMap();
                switch (orderType) {
                    case 1:
                        try {
                            return MapUtils.CompareMapWithMapFirstType(o1, o2);
                        } catch (IOException e) {
                            e.printStackTrace();
                            System.out.println("Error occurred during map comparison!");
                        }
                        break;
                    case 2:
                        try {
                            return MapUtils.CompareMapWithMapSecondType(o1, o2);
                        } catch (IOException e) {
                            e.printStackTrace();
                            System.out.println("Error occurred during map comparison!");
                        }
                        break;
                    case 3:
                        try {
                            return MapUtils.CompareMapWithMapThirdType(o1, o2);
                        } catch (IOException e) {
                            e.printStackTrace();
                            System.out.println("Error occurred during map comparison!");
                        }
                        break;
                    case 4:
                        try {
                            return MapUtils.CompareMapWithMapFourthType(o1, o2);
                        } catch (IOException e) {
                            e.printStackTrace();
                            System.out.println("Error occurred during map comparison!");
                        }
                        break;
                    case 5:
                        try {
                            return MapUtils.CompareMapWithMapFifthType(o1, o2);
                        } catch (IOException e) {
                            e.printStackTrace();
                            System.out.println("Error occurred during map comparison!");
                        }
                        break;
                }
                return 0;
            }
        });
        for(int i=0; i<5; i++) {
            try {
                scanHeapFiles[i] = heapFiles[i].openScanMap();
            } catch(Exception e) {
                e.printStackTrace();
                System.out.println("Error occurred while initiating scan for " + i + " th heap file");
            }
        }
    }

    public Map getNext() {
        if(smallestScanIndex == null) {
            for(int i=0; i<5; i++) {
                try {
                    Map temp = scanHeapFiles[i].getNextMap(new MID());
                    if(temp != null) {
                        temp.setFldOffset(temp.getMapByteArray());
                        pq.add(new Pair(temp, null, null, i));
                    }
                } catch(Exception e) {
                    e.printStackTrace();
                    System.out.println("Error occurred while scanning " + i + " th heap file");
                }
            }
            if(pq.isEmpty()) {
                return null;
            } else {
                // Record the index
                smallestScanIndex = pq.peek().getHeapFileIndex();
                return pq.poll().getMap();
            }
        } else {
            try {
                Map temp = scanHeapFiles[smallestScanIndex].getNextMap(new MID());
                if(temp != null) {
                    temp.setFldOffset(temp.getMapByteArray());
                    pq.add(new Pair(temp, null, null, smallestScanIndex));
                }
                if(pq.isEmpty()) {
                    return null;
                } else {
                    smallestScanIndex = pq.peek().getHeapFileIndex();
                    return pq.poll().getMap();
                }
            } catch(Exception e) {
                e.printStackTrace();
                System.out.println("Error occurred while scanning " + smallestScanIndex + " th heap file");
            }
        }
        return null;
    }

    public void closeCombinedStream() {
        for(int i=0; i<5; i++) {
            try {
                scanHeapFiles[i].closescan();
                heapFiles[i].deleteFileMap();
            } catch(Exception e) {
                e.printStackTrace();
                System.out.println("Error occurred while deleting " + i + " th heap file");
            }
        }
    }
}