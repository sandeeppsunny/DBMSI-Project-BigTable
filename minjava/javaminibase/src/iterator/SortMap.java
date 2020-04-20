package iterator;

import java.io.*;

import BigT.Map;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import index.*;
import chainexception.*;

/**
 * The Sort class sorts a file. All necessary information are passed as
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get maps in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class SortMap extends MapIterator implements GlobalConst {
    private static final int ARBIT_RUNS = 10;

    private AttrType[] _in;
    private short n_cols;
    private short[] str_lens;
    private MapIterator _am;
    private int _sort_fld;
    private MapOrder order;
    private int _n_pages;
    private byte[][] bufs;
    private boolean first_time;
    private int Nruns;
    private int max_elems_in_heap;
    private int sortFldLen;
    private int map_size;
    private int order_type;

    private pnodeSplayPQMap Q;
    private Heapfile[] temp_files;
    private int n_tempfiles;
    private Map output_map;
    private int[] n_maps;
    private int n_runs;
    private Map op_buf;
    private OBufMap o_buf;
    private SpoofIbufMap[] i_buf;
    private PageId[] bufs_pids;
    private boolean useBM = true; // flag for whether to use buffer manager

    /**
     * Set up for merging the runs.
     * Open an input buffer for each run, and insert the first element (min)
     * from each run into a heap. <code>delete_min() </code> will then get
     * the minimum of all runs.
     *
     * @param map_size size (in bytes) of each map
     * @param n_R_runs   number of runs
     * @throws IOException     from lower layers
     * @throws LowMemException there is not enough memory to
     *                         sort in two passes (a subclass of SortException).
     * @throws SortException   something went wrong in the lower layer.
     * @throws Exception       other exceptions
     */
    private void setup_for_merge(int map_size, int n_R_runs)
            throws IOException,
            LowMemException,
            SortException,
            Exception {
        // don't know what will happen if n_R_runs > _n_pages
        if (n_R_runs > _n_pages)
            throw new LowMemException("Sort.java: Not enough memory to sort in two passes.");

        int i;
        pnodeMap cur_node;  // need pq_defs.java

        i_buf = new SpoofIbufMap[n_R_runs];   // need io_bufs.java
        for (int j = 0; j < n_R_runs; j++) i_buf[j] = new SpoofIbufMap();

        // construct the lists, ignore TEST for now
        // this is a patch, I am not sure whether it works well -- bingjie 4/20/98

        for (i = 0; i < n_R_runs; i++) {
            byte[][] apage = new byte[1][];
            apage[0] = bufs[i];

            // need iobufs.java
            i_buf[i].init(temp_files[i], apage, 1, map_size, n_maps[i]);

            cur_node = new pnodeMap();
            cur_node.run_num = i;

            // may need change depending on whether Get() returns the original
            // or make a copy of the map, need io_bufs.java ???
            Map temp_map = new Map(map_size);

            try {
                // temp_map.setHdr(n_cols, _in, str_lens);
                temp_map.setDefaultHdr();
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: Tuple.setHdr() failed");
            }

            temp_map = i_buf[i].Get(temp_map);  // need io_bufs.java

            if (temp_map != null) {
	/*
	System.out.print("Get tuple from run " + i);
	temp_tuple.print(_in);
	*/
                cur_node.map = temp_map; // no copy needed
                try {
                    Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                } catch (TupleUtilsException e) {
                    throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
                }

            }
        }
        return;
    }

    /**
     * Generate sorted runs.
     * Using heap sort.
     *
     * @param max_elems   maximum number of elements in heap
     * @param sortFldType attribute type of the sort field
     * @param sortFldLen  length of the sort field
     * @return number of runs generated
     * @throws IOException    from lower layers
     * @throws SortException  something went wrong in the lower layer.
     * @throws JoinsException from <code>Iterator.get_next()</code>
     */
    private int generate_runs(int max_elems, AttrType sortFldType, int sortFldLen)
            throws IOException,
            SortException,
            UnknowAttrType,
            TupleUtilsException,
            JoinsException,
            Exception {
        Map map;
        pnodeMap cur_node;
        pnodeSplayPQMap Q1 = new pnodeSplayPQMap(_sort_fld, order_type, sortFldType, order);
        pnodeSplayPQMap Q2 = new pnodeSplayPQMap(_sort_fld, order_type, sortFldType, order);
        pnodeSplayPQMap pcurr_Q = Q1;
        pnodeSplayPQMap pother_Q = Q2;
        Map lastElem = new Map(map_size);  // need tuple.java
        try {
            // lastElem.setHdr(n_cols, _in, str_lens);
            lastElem.setDefaultHdr();
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: setHdr() failed");
        }

        int run_num = 0;  // keeps track of the number of runs

        // number of elements in Q
        //    int nelems_Q1 = 0;
        //    int nelems_Q2 = 0;
        int p_elems_curr_Q = 0;
        int p_elems_other_Q = 0;

        int comp_res;

        // set the lastElem to be the minimum value for the sort field
        if (order.mapOrder == MapOrder.Ascending) {
            try {
                MIN_VAL(lastElem, sortFldType);
            } catch (UnknowAttrType e) {
                throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
            } catch (Exception e) {
                throw new SortException(e, "MIN_VAL failed");
            }
        } else {
            try {
                MAX_VAL(lastElem, sortFldType);
            } catch (UnknowAttrType e) {
                throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
            } catch (Exception e) {
                throw new SortException(e, "MIN_VAL failed");
            }
        }

        // maintain a fixed maximum number of elements in the heap
        while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
            try {
                map = _am.get_next();  // according to Iterator.java
            } catch (Exception e) {
                e.printStackTrace();
                throw new SortException(e, "Sort.java: get_next() failed");
            }

            if (map == null) {
                break;
            }
            cur_node = new pnodeMap();
            cur_node.map = new Map(map); // tuple copy needed --  Bingjie 4/29/98

            pcurr_Q.enq(cur_node);
            p_elems_curr_Q++;
        }

        // now the queue is full, starting writing to file while keep trying
        // to add new tuples to the queue. The ones that does not fit are put
        // on the other queue temporarily
        while (true) {
            cur_node = pcurr_Q.deq();
            if (cur_node == null) break;
            p_elems_curr_Q--;

            //comp_res = TupleUtils.CompareTupleWithValue(sortFldType, cur_node.tuple, _sort_fld, lastElem);  // need tuple_utils.java

            comp_res = MapUtils.CompareMapWithMap(cur_node.map, lastElem, _sort_fld);  // need tuple_utils.java
            switch(order_type) {
                case 1:
                    comp_res = MapUtils.CompareMapWithMapFirstType(cur_node.map, lastElem);
                    break;
                case 2:
                    comp_res = MapUtils.CompareMapWithMapSecondType(cur_node.map, lastElem);
                    break;
                case 3:
                    comp_res = MapUtils.CompareMapWithMapThirdType(cur_node.map, lastElem);
                    break;
                case 4:
                    comp_res = MapUtils.CompareMapWithMapFourthType(cur_node.map, lastElem);
                    break;
                case 5:
                    comp_res = MapUtils.CompareMapWithMapFifthType(cur_node.map, lastElem);
                    break;
                case 6:
                    comp_res = MapUtils.CompareMapWithMapSixthType(cur_node.map, lastElem);
                    break;
                case 7:
                    comp_res = MapUtils.CompareMapWithMapValues(cur_node.map, lastElem);
                    break;
            }

            if ((comp_res < 0 && order.mapOrder == MapOrder.Ascending) || (comp_res > 0 && order.mapOrder == MapOrder.Descending)) {
                // doesn't fit in current grun, put into the other queue
                try {
                    pother_Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                }
                p_elems_other_Q++;
            } else {
                // set lastElem to have the value of the current tuple,
                // need tuple_utils.java
                cur_node.map.setFldOffset(cur_node.map.getMapByteArray());
                lastElem.setRowLabel(cur_node.map.getRowLabel());
                lastElem.setColumnLabel(cur_node.map.getColumnLabel());
                lastElem.setTimeStamp(cur_node.map.getTimeStamp());
                lastElem.setValue(cur_node.map.getValue());

                // TupleUtils.SetValue(lastElem, cur_node.map, _sort_fld, sortFldType);
                // write tuple to output file, need io_bufs.java, type cast???
                //	System.out.println("Putting tuple into run " + (run_num + 1));
                //	cur_node.tuple.print(_in);

                o_buf.Put(cur_node.map);
            }

            // check whether the other queue is full
            if (p_elems_other_Q == max_elems) {
                // close current run and start next run
                n_maps[run_num] = (int) o_buf.flush();  // need io_bufs.java
                run_num++;

                // check to see whether need to expand the array
                if (run_num == n_tempfiles) {
                    Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
                    for (int i = 0; i < n_tempfiles; i++) {
                        temp1[i] = temp_files[i];
                    }
                    temp_files = temp1;
                    n_tempfiles *= 2;

                    int[] temp2 = new int[2 * n_runs];
                    for (int j = 0; j < n_runs; j++) {
                        temp2[j] = n_maps[j];
                    }
                    n_maps = temp2;
                    n_runs *= 2;
                }

                try {
                    temp_files[run_num] = new Heapfile(null);
                } catch (Exception e) {
                    throw new SortException(e, "Sort.java: create Heapfile failed");
                }

                // need io_bufs.java
                o_buf.init(bufs, _n_pages, map_size, temp_files[run_num], false);

                // set the last Elem to be the minimum value for the sort field
                if (order.mapOrder == MapOrder.Ascending) {
                    try {
                        MIN_VAL(lastElem, sortFldType);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                    } catch (Exception e) {
                        throw new SortException(e, "MIN_VAL failed");
                    }
                } else {
                    try {
                        MAX_VAL(lastElem, sortFldType);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                    } catch (Exception e) {
                        throw new SortException(e, "MIN_VAL failed");
                    }
                }

                // switch the current heap and the other heap
                pnodeSplayPQMap tempQ = pcurr_Q;
                pcurr_Q = pother_Q;
                pother_Q = tempQ;
                int tempelems = p_elems_curr_Q;
                p_elems_curr_Q = p_elems_other_Q;
                p_elems_other_Q = tempelems;
            }

            // now check whether the current queue is empty
            else if (p_elems_curr_Q == 0) {
                while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
                    try {
                        map = _am.get_next();  // according to Iterator.java
                    } catch (Exception e) {
                        throw new SortException(e, "get_next() failed");
                    }

                    if (map == null) {
                        break;
                    }
                    cur_node = new pnodeMap();
                    cur_node.map = new Map(map); // tuple copy needed --  Bingjie 4/29/98

                    try {
                        pcurr_Q.enq(cur_node);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                    }
                    p_elems_curr_Q++;
                }
            }

            // Check if we are done
            if (p_elems_curr_Q == 0) {
                // current queue empty despite our attemps to fill in
                // indicating no more tuples from input
                if (p_elems_other_Q == 0) {
                    // other queue is also empty, no more tuples to write out, done
                    break; // of the while(true) loop
                } else {
                    // generate one more run for all tuples in the other queue
                    // close current run and start next run
                    n_maps[run_num] = (int) o_buf.flush();  // need io_bufs.java
                    run_num++;

                    // check to see whether need to expand the array
                    if (run_num == n_tempfiles) {
                        Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
                        for (int i = 0; i < n_tempfiles; i++) {
                            temp1[i] = temp_files[i];
                        }
                        temp_files = temp1;
                        n_tempfiles *= 2;

                        int[] temp2 = new int[2 * n_runs];
                        for (int j = 0; j < n_runs; j++) {
                            temp2[j] = n_maps[j];
                        }
                        n_maps = temp2;
                        n_runs *= 2;
                    }

                    try {
                        temp_files[run_num] = new Heapfile(null);
                    } catch (Exception e) {
                        throw new SortException(e, "Sort.java: create Heapfile failed");
                    }

                    // need io_bufs.java
                    o_buf.init(bufs, _n_pages, map_size, temp_files[run_num], false);

                    // set the last Elem to be the minimum value for the sort field
                    if (order.mapOrder == MapOrder.Ascending) {
                        try {
                            MIN_VAL(lastElem, sortFldType);
                        } catch (UnknowAttrType e) {
                            throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                        } catch (Exception e) {
                            throw new SortException(e, "MIN_VAL failed");
                        }
                    } else {
                        try {
                            MAX_VAL(lastElem, sortFldType);
                        } catch (UnknowAttrType e) {
                            throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                        } catch (Exception e) {
                            throw new SortException(e, "MIN_VAL failed");
                        }
                    }

                    // switch the current heap and the other heap
                    pnodeSplayPQMap tempQ = pcurr_Q;
                    pcurr_Q = pother_Q;
                    pother_Q = tempQ;
                    int tempelems = p_elems_curr_Q;
                    p_elems_curr_Q = p_elems_other_Q;
                    p_elems_other_Q = tempelems;
                }
            } // end of if (p_elems_curr_Q == 0)
        } // end of while (true)

        // close the last run
        n_maps[run_num] = (int) o_buf.flush();
        run_num++;

        return run_num;
    }

    /**
     * Remove the minimum value among all the runs.
     *
     * @return the minimum map removed
     * @throws IOException   from lower layers
     * @throws SortException something went wrong in the lower layer.
     */
    private Map delete_min()
            throws IOException,
            SortException,
            Exception {
        pnodeMap cur_node;                // needs pq_defs.java
        Map new_map, old_map;

        cur_node = Q.deq();
        old_map = cur_node.map;
    /*
    System.out.print("Get ");
    old_tuple.print(_in);
    */
        // we just removed one map from one run, now we need to put another
        // tuple of the same run into the queue
        if (i_buf[cur_node.run_num].empty() != true) {
            // run not exhausted
            new_map = new Map(map_size); // need tuple.java??

            try {
                // new_map.setHdr(n_cols, _in, str_lens);
                new_map.setDefaultHdr();
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: setHdr() failed");
            }

            new_map = i_buf[cur_node.run_num].Get(new_map);
            if (new_map != null) {
	/*
	System.out.print(" fill in from run " + cur_node.run_num);
	new_tuple.print(_in);
	*/
                cur_node.map = new_map;  // no copy needed -- I think Bingjie 4/22/98
                try {
                    Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                } catch (TupleUtilsException e) {
                    throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
                }
            } else {
                throw new SortException("********** Wait a minute, I thought input is not empty ***************");
            }

        }

        // changed to return Tuple instead of return char array ????
        return old_map;
    }

    /**
     * Set lastElem to be the minimum value of the appropriate type
     *
     * @param lastElem    the map
     * @param sortFldType the sort field type
     * @throws IOException    from lower layers
     * @throws UnknowAttrType attrSymbol or attrNull encountered
     */
    private void MIN_VAL(Map lastElem, AttrType sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
        //    AttrType[] junk = new AttrType[1];
        //    junk[0] = new AttrType(sortFldType.attrType);
        char[] c = new char[1];
        c[0] = Character.MIN_VALUE;
        String s = new String(c);
        //    short fld_no = 1;

        /*
        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setIntFld(_sort_fld, Integer.MIN_VALUE);
                break;
            case AttrType.attrReal:
                //      lastElem.setHdr(fld-no, junk, null);
                lastElem.setFloFld(_sort_fld, Float.MIN_VALUE);
                break;
            case AttrType.attrString:
                //      lastElem.setHdr(fld_no, junk, s_size);
                lastElem.setStrFld(_sort_fld, s);
                break;
            default:
                // don't know how to handle attrSymbol, attrNull
                //System.err.println("error in sort.java");
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }

        switch(_sort_fld) {
            case 1:
                lastElem.setRowLabel(s);
                break;
            case 2:
                lastElem.setColumnLabel(s);
                break;
            case 3:
                lastElem.setTimeStamp(Integer.MIN_VALUE);
                break;
            case 4:
                lastElem.setValue(s);
                break;
            default:
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }
                */
        lastElem.setRowLabel(s);
        lastElem.setColumnLabel(s);
        lastElem.setTimeStamp(Integer.MIN_VALUE);
        lastElem.setValue(s);
        return;
    }

    /**
     * Set lastElem to be the maximum value of the appropriate type
     *
     * @param lastElem    the map
     * @param sortFldType the sort field type
     * @throws IOException    from lower layers
     * @throws UnknowAttrType attrSymbol or attrNull encountered
     */
    private void MAX_VAL(Map lastElem, AttrType sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
        //    AttrType[] junk = new AttrType[1];
        //    junk[0] = new AttrType(sortFldType.attrType);
        char[] c = new char[1];
        c[0] = Character.MAX_VALUE;
        String s = new String(c);
        //    short fld_no = 1;

        /*
        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setIntFld(_sort_fld, Integer.MAX_VALUE);
                break;
            case AttrType.attrReal:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setFloFld(_sort_fld, Float.MAX_VALUE);
                break;
            case AttrType.attrString:
                //      lastElem.setHdr(fld_no, junk, s_size);
                lastElem.setStrFld(_sort_fld, s);
                break;
            default:
                // don't know how to handle attrSymbol, attrNull
                //System.err.println("error in sort.java");
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }

        switch(_sort_fld) {
            case 1:
                lastElem.setRowLabel(s);
                break;
            case 2:
                lastElem.setColumnLabel(s);
                break;
            case 3:
                lastElem.setTimeStamp(Integer.MAX_VALUE);
                break;
            case 4:
                lastElem.setValue(s);
                break;
            default:
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }
        */
        lastElem.setRowLabel(s);
        lastElem.setColumnLabel(s);
        lastElem.setTimeStamp(Integer.MAX_VALUE);
        lastElem.setValue("456789000");
        return;
    }

    /**
     * Class constructor, take information about the maps, and set up
     * the sorting
     *
     * @param in_temp            array containing attribute types of the relation
     * @param len_in_temp         number of columns in the relation
     * @param str_sizes_temp      array of sizes of string attributes
     * @param am             an iterator for accessing the maps
     * @param sort_fld       the field number of the field to sort on
     * @param sort_order     the sorting order (ASCENDING, DESCENDING)
     * @param sort_fld_len the length of the sort field
     * @param n_pages        amount of memory (in pages) available for sorting
     * @throws IOException   from lower layers
     * @throws SortException something went wrong in the lower layer.
     */
    public SortMap(AttrType[] in_temp,
                Short len_in_temp,
                short[] str_sizes_temp,
                MapIterator am,
                int sort_fld,
                MapOrder sort_order,
                Integer sort_fld_len,
                int n_pages
    ) throws IOException, SortException {
        short len_in = 4;
        AttrType[] in = new AttrType[4];
        in[0] = new AttrType(AttrType.attrString);
        in[1] = new AttrType(AttrType.attrString);
        in[2] = new AttrType(AttrType.attrInteger);
        in[3] = new AttrType(AttrType.attrString);
        short[] str_sizes = new short[4];
        str_sizes[0] = Map.DEFAULT_STRING_ATTRIBUTE_SIZE;
        str_sizes[1] = Map.DEFAULT_STRING_ATTRIBUTE_SIZE;
//        str_sizes[2] = 4;
        str_sizes[2] = Map.DEFAULT_STRING_ATTRIBUTE_SIZE;
        str_sizes[3] = Map.DEFAULT_STRING_ATTRIBUTE_SIZE;
        _in = new AttrType[len_in];
        n_cols = len_in;
        int n_strs = 0;

        switch(sort_fld) {
            case 2:
                _sort_fld = 2;
                order_type = 2;
                break;
            case 3:
                _sort_fld = 1;
                order_type = 3;
                break;
            case 4:
                _sort_fld = 2;
                order_type = 4;
                break;
            case 5:
                _sort_fld = 3;
                order_type = 5;
                break;
            case 6:
                _sort_fld = 1;
                order_type = 6;
                break;
            case 7:
                _sort_fld = 4;
                order_type = 7;
                break;
            default:
                // case 1 and default maps to first order type
                _sort_fld = 1;
                order_type = 1;
        }

        for (int i = 0; i < len_in; i++) {
            _in[i] = new AttrType(in[i].attrType);
            if (in[i].attrType == AttrType.attrString) {
                n_strs++;
            }
        }

        str_lens = new short[n_strs];

        n_strs = 0;
        for (int i = 0; i < len_in; i++) {
            if (_in[i].attrType == AttrType.attrString) {
                str_lens[n_strs] = str_sizes[n_strs];
                n_strs++;
            }
        }

        Map t = new Map(); // need Tuple.java
        try {
            // t.setHdr(len_in, _in, str_sizes);
            t.setDefaultHdr();
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: t.setHdr() failed");
        }
        map_size = t.size();

        _am = am;
        order = sort_order;
        _n_pages = n_pages;

        // this may need change, bufs ???  need io_bufs.java
        //    bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
        bufs_pids = new PageId[_n_pages];
        bufs = new byte[_n_pages][];

        if (useBM) {
            try {
                get_buffer_pages(_n_pages, bufs_pids, bufs);
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: BUFmgr error");
            }
        } else {
            for (int k = 0; k < _n_pages; k++) bufs[k] = new byte[MAX_SPACE];
        }

        first_time = true;

        // as a heuristic, we set the number of runs to an arbitrary value
        // of ARBIT_RUNS
        temp_files = new Heapfile[ARBIT_RUNS];
        n_tempfiles = ARBIT_RUNS;
        n_maps = new int[ARBIT_RUNS];
        n_runs = ARBIT_RUNS;

        try {
            temp_files[0] = new Heapfile(null);
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: Heapfile error");
        }

        o_buf = new OBufMap();

        o_buf.init(bufs, _n_pages, map_size, temp_files[0], false);
        //    output_tuple = null;

        max_elems_in_heap = 5000;
        sortFldLen = str_sizes[_sort_fld-1];

        Q = new pnodeSplayPQMap(_sort_fld, order_type, in[_sort_fld - 1], order);

        op_buf = new Map(map_size);   // need Tuple.java
        try {
            // op_buf.setHdr(n_cols, _in, str_lens);
            op_buf.setDefaultHdr();
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
        }
    }

    /**
     * Returns the next map in sorted order.
     * Note: You need to copy out the content of the map, otherwise it
     * will be overwritten by the next <code>get_next()</code> call.
     *
     * @return the next map, null if all maps exhausted
     * @throws IOException     from lower layers
     * @throws SortException   something went wrong in the lower layer.
     * @throws JoinsException  from <code>generate_runs()</code>.
     * @throws UnknowAttrType  attribute type unknown
     * @throws LowMemException memory low exception
     * @throws Exception       other exceptions
     */
    public Map get_next()
            throws IOException,
            SortException,
            UnknowAttrType,
            LowMemException,
            JoinsException,
            Exception {
        if (first_time) {
            // first get_next call to the sort routine
            first_time = false;

            // generate runs
            Nruns = generate_runs(max_elems_in_heap, _in[_sort_fld - 1], sortFldLen);
            //      System.out.println("Generated " + Nruns + " runs");

            // setup state to perform merge of runs.
            // Open input buffers for all the input file
            setup_for_merge(map_size, Nruns);
        }

        if (Q.empty()) {
            // no more tuples availble
            return null;
        }

        output_map = delete_min();
        if (output_map != null) {
            op_buf.mapCopy(output_map);
            return op_buf;
        } else
            return null;
    }

    /**
     * Cleaning up, including releasing buffer pages from the buffer pool
     * and removing temporary files from the database.
     *
     * @throws IOException   from lower layers
     * @throws SortException something went wrong in the lower layer.
     */
    public void close() throws SortException, IOException {
        // clean up
        if (!closeFlag) {

            try {
                _am.close();
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: error in closing iterator.");
            }

            if (useBM) {
                try {
                    free_buffer_pages(_n_pages, bufs_pids);
                } catch (Exception e) {
                    throw new SortException(e, "Sort.java: BUFmgr error");
                }
                for (int i = 0; i < _n_pages; i++) bufs_pids[i].pid = INVALID_PAGE;
//                System.out.println("After Free Buffer Pages in Sort - close");
//                System.out.println("Number of unpinned Buffers " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
//                System.out.println("Number of buffers " + SystemDefs.JavabaseBM.getNumBuffers());
            }
//            System.out.println("Number of Temporary maps used: " + temp_files.length);

            for (int i = 0; i < temp_files.length; i++) {
                if (temp_files[i] != null) {
                    try {
                        temp_files[i].deleteFileMap();
                    } catch (Exception e) {
                        throw new SortException(e, "Sort.java: Heapfile error");
                    }
                    temp_files[i] = null;
                }
            }
            closeFlag = true;
        }
    }

}
