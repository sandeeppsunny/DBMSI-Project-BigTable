package BigT;

import global.MID;
import heap.Scan;
import java.io.*;

public class Stream implements GlobalConst {

	private Heapfile  _hf;

	private PageId dirpageId = new PageId();

    /** pointer to in-core data of dirpageId (page is pinned) */
    private HFPage dirpage = new HFPage();

    /** record ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current record lives.
     */
    private MID datapageMid = new MID();

    /** the actual PageId of the data page with the current record */
    private PageId datapageId = new PageId();

    /** in-core copy (pinned) of the same */
    private HFPage datapage = new HFPage();

    /** record ID of the current record (from the current data page) */
    private MID usermid = new MID();

    /** Status of next user status */
    private boolean nextUserStatus;

	public Stream(bigt bigtable, int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.lang.String valueFilter)  {
		
		firstDataPage();


	}
	
	public void closeStream() {
		reset();
		
	}

	private void reset()
    { 

	    if (datapage != null) {
	    
	    try{
	      unpinPage(datapageId, false);
	    }
	    catch (Exception e){
	      // 	System.err.println("SCAN: Error in Scan" + e);
	      e.printStackTrace();
	    }  
	    }
	    datapageId.pid = 0;
	    datapage = null;

	    if (dirpage != null) {
	    
	      try{
		unpinPage(dirpageId, false);
	      }
	      catch (Exception e){
		//     System.err.println("SCAN: Error in Scan: " + e);
		e.printStackTrace();
	      }
	    }
	    dirpage = null;
	 
	    nextUserStatus = true;

	  }

	private void unpinPage(PageId pageno, boolean dirty)
	    throws HFBufMgrException {

	    try {
	      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
	    }
	    catch (Exception e) {
	      throw new HFBufMgrException(e,"Scan.java: unpinPage() failed");
	    }

	  }
	
	public Map getNext(MID mid) {

	Map recptrmap = null;
    
    if (nextUserStatus != true) {
        nextDataPage();
    }
     
    if (datapage == null)
      return null;
    
    mid.pageNo.pid = usermid.pageNo.pid;    
    mid.slotNo = usermid.slotNo;
         
    try {
      recptrmap = datapage.getRecord(mid);
    }
    
    catch (Exception e) {
  //    System.err.println("SCAN: Error in Scan" + e);
      e.printStackTrace();
    }   
    
    usermid = datapage.nextRecord(mid);
    if(usermid == null) nextUserStatus = false;
    else nextUserStatus = true;
     
    return recptrmap;

	}

	private boolean firstDataPage() 
    throws InvalidTupleSizeException,
	   IOException
  {
    DataPageInfo dpinfo;
    Map        recmap = null;
    Boolean      bst;

    /** copy data about first directory page */
 
    dirpageId.pid = _hf._firstDirPageId.pid;  
    nextUserStatus = true;

    /** get first directory page and pin it */
    	try {
	   dirpage  = new HFPage();
       	   pinPage(dirpageId, (Page) dirpage, false);	   
       }

    	catch (Exception e) {
    //    System.err.println("SCAN Error, try pinpage: " + e);
	e.printStackTrace();
	}
    
    /** now try to get a pointer to the first datapage */
	 datapageMid = dirpage.firstRecord();
	 
    	if (datapageMid != null) {
    /** there is a datapage record on the first directory page: */
	
	try {
          recmap = dirpage.getRecord(datapageMid);
	}  
				
	catch (Exception e) {
	//	System.err.println("SCAN: Chain Error in Scan: " + e);
		e.printStackTrace();
	}		
      			    
    	dpinfo = new DataPageInfo(recmap);
        datapageId.pid = dpinfo.pageId.pid;

    } else {

    /** the first directory page is the only one which can possibly remain
     * empty: therefore try to get the next directory page and
     * check it. The next one has to contain a datapage record, unless
     * the heapfile is empty:
     */
      PageId nextDirPageId = new PageId();
      
      nextDirPageId = dirpage.getNextPage();
      
      if (nextDirPageId.pid != INVALID_PAGE) {
	
	try {
            unpinPage(dirpageId, false);
            dirpage = null;
	    }
	
	catch (Exception e) {
	//	System.err.println("SCAN: Error in 1stdatapage 1 " + e);
		e.printStackTrace();
	}
        	
	try {
	
           dirpage = new HFPage();
	    pinPage(nextDirPageId, (Page )dirpage, false);
	
	    }
	
	catch (Exception e) {
	//  System.err.println("SCAN: Error in 1stdatapage 2 " + e);
	  e.printStackTrace();
	}
	
	/** now try again to read a data record: */
	
	try {
	  datapageMid = dirpage.firstRecord();
	}
        
	catch (Exception e) {
	//  System.err.println("SCAN: Error in 1stdatapg 3 " + e);
	  e.printStackTrace();
	  datapageId.pid = INVALID_PAGE;
	}
       
	if(datapageMid != null) {
          
	  try {
	  
	    recmap = dirpage.getRecord(datapageMid);
	  }
	  
	  catch (Exception e) {
	//    System.err.println("SCAN: Error getRecord 4: " + e);
	    e.printStackTrace();
	  }
	  
	  if (recmap.getLength() != DataPageInfo.size)
	    return false;
	  
	  dpinfo = new DataPageInfo(recmap);
	  datapageId.pid = dpinfo.pageId.pid;
	  
         } else {
	   // heapfile empty
           datapageId.pid = INVALID_PAGE;
         }
       }//end if01
       else {// heapfile empty
	datapageId.pid = INVALID_PAGE;
	}
}	
	
	datapage = null;

	try{
         nextDataPage();
	  }
	  
	catch (Exception e) {
	//  System.err.println("SCAN Error: 1st_next 0: " + e);
	  e.printStackTrace();
	}
	
      return true;
      
      /** ASSERTIONS:
       * - first directory page pinned
       * - this->dirpageId has Id of first directory page
       * - this->dirpage valid
       * - if heapfile empty:
       *    - this->datapage == NULL, this->datapageId==INVALID_PAGE
       * - if heapfile nonempty:
       *    - this->datapage == NULL, this->datapageId, this->datapageMid valid
       *    - first datapage is not yet pinned
       */
    
  }
    

  /** Move to the next data page in the file and 
   * retrieve the next data page. 
   *
   * @return 		true if successful
   *			false if unsuccessful
   */

	private boolean nextDataPage() 
    throws InvalidTupleSizeException,
	   IOException
  {
    DataPageInfo dpinfo;
    
    boolean nextDataPageStatus;
    PageId nextDirPageId = new PageId();
    Map recmap = null;

  // ASSERTIONS:
  // - this->dirpageId has Id of current directory page
  // - this->dirpage is valid and pinned
  // (1) if heapfile empty:
  //    - this->datapage==NULL; this->datapageId == INVALID_PAGE
  // (2) if overall first record in heapfile:
  //    - this->datapage==NULL, but this->datapageId valid
  //    - this->datapageMid valid
  //    - current data page unpinned !!!
  // (3) if somewhere in heapfile
  //    - this->datapageId, this->datapage, this->datapageMid valid
  //    - current data page pinned
  // (4)- if the scan had already been done,
  //        dirpage = NULL;  datapageId = INVALID_PAGE
    
    if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
        return false;

    if (datapage == null) {
      if (datapageId.pid == INVALID_PAGE) {
	// heapfile is empty to begin with
	
	try{
	  unpinPage(dirpageId, false);
	  dirpage = null;
	}
	catch (Exception e){
	//  System.err.println("Scan: Chain Error: " + e);
	  e.printStackTrace();
	}
	
      } else {
	
	// pin first data page
	try {
	  datapage  = new HFPage();
	  pinPage(datapageId, (Page) datapage, false);
	}
	catch (Exception e){
	  e.printStackTrace();
	}
	
	try {
	  usermid = datapage.firstRecord();
	}
	catch (Exception e) {
	  e.printStackTrace();
	}
	
	return true;
        }
    }
  
  // ASSERTIONS:
  // - this->datapage, this->datapageId, this->datapageMid valid
  // - current datapage pinned

    // unpin the current datapage
    try{
      unpinPage(datapageId, false /* no dirty */);
        datapage = null;
    }
    catch (Exception e){
      
    }
          
    // read next datapagerecord from current directory page
    // dirpage is set to NULL at the end of scan. Hence
    
    if (dirpage == null) {
      return false;
    }
    
    datapageMid = dirpage.nextRecord(datapageMid);
    
    if (datapageMid == null) {
      nextDataPageStatus = false;
      // we have read all datapage records on the current directory page
      
      // get next directory page
      nextDirPageId = dirpage.getNextPage();
  
      // unpin the current directory page
      try {
	unpinPage(dirpageId, false /* not dirty */);
	dirpage = null;
	
	datapageId.pid = INVALID_PAGE;
      }
      
      catch (Exception e) {
	
      }
		    
      if (nextDirPageId.pid == INVALID_PAGE)
	return false;
      else {
	// ASSERTION:
	// - nextDirPageId has correct id of the page which is to get
	
	dirpageId = nextDirPageId;
	
 	try { 
	  dirpage  = new HFPage();
	  pinPage(dirpageId, (Page)dirpage, false);
	}
	
	catch (Exception e){
	  
	}
	
	if (dirpage == null)
	  return false;
	
    	try {
	  datapageMid = dirpage.firstRecord();
	  nextDataPageStatus = true;
	}
	catch (Exception e){
	  nextDataPageStatus = false;
	  return false;
	} 
      }
    }
    
    // ASSERTION:
    // - this->dirpageId, this->dirpage valid
    // - this->dirpage pinned
    // - the new datapage to be read is on dirpage
    // - this->datapageMid has the Rid of the next datapage to be read
    // - this->datapage, this->datapageId invalid
  
    // data page is not yet loaded: read its record from the directory page
   	try {
	  recmap = dirpage.getRecord(datapageMid);
	}
	
	catch (Exception e) {
	  System.err.println("HeapFile: Error in Scan" + e);
	}
	
	if (recmap.getLength() != DataPageInfo.size)
	  return false;
                        
	dpinfo = new DataPageInfo(recmap);
	datapageId.pid = dpinfo.pageId.pid;
	
 	try {
	  datapage = new HFPage();
	  pinPage(dpinfo.pageId, (Page) datapage, false);
	}
	
	catch (Exception e) {
	  System.err.println("HeapFile: Error in Scan" + e);
	}
	
     
     // - directory page is pinned
     // - datapage is pinned
     // - this->dirpageId, this->dirpage correct
     // - this->datapageId, this->datapage, this->datapageMid correct

     usermid = datapage.firstRecord();
     
     if(usermid == null)
     {
       nextUserStatus = false;
       return false;
     }
  
     return true;
  }






private boolean peekNext(MID mid) {
    
    mid.pageNo.pid = usermid.pageNo.pid;
    mid.slotNo = usermid.slotNo;
    return true;
    
  }


  /** Move to the next record in a sequential scan.
   * Also returns the RID of the (new) current record.
   */
  private boolean mvNext(MID mid) 
    throws InvalidTupleSizeException,
	   IOException
  {
    MID nextmid;
    boolean status;

    if (datapage == null)
        return false;

    	nextmid = datapage.nextRecord(mid);
	
	if( nextmid != null ){
	  usermid.pageNo.pid = nextmid.pageNo.pid;
	  usermid.slotNo = nextmid.slotNo;
	  return true;
	} else {
	  
	  status = nextDataPage();

	  if (status==true){
	    mid.pageNo.pid = usermid.pageNo.pid;
	    mid.slotNo = usermid.slotNo;
	  }
	
	}
	return true;
  }

    /**
   * short cut to access the pinPage function in bufmgr package.
   * @see bufmgr.pinPage
   */
  private void pinPage(PageId pageno, Page page, boolean emptyPage)
    throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Scan.java: pinPage() failed");
    }

  } // end of pinPage

  /**
   * short cut to access the unpinPage function in bufmgr package.
   * @see bufmgr.unpinPage
   */
  private void unpinPage(PageId pageno, boolean dirty)
    throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Scan.java: unpinPage() failed");
    }

  } // end of unpinPage

  public boolean position(MID mid) 
    throws InvalidTupleSizeException,
	   IOException
  { 
    MID    nxtmid = new MID();
    boolean bst;

    bst = peekNext(nxtmid);

    if (nxtmid.equals(mid)==true) 
    	return true;

    // This is kind lame, but otherwise it will take all day.
    PageId pgid = new PageId();
    pgid.pid = mid.pageNo.pid;
 
    if (!datapageId.equals(pgid)) {

      // reset everything and start over from the beginning
      reset();
      
      bst =  firstDataPage();

      if (bst != true)
	return bst;
      
      while (!datapageId.equals(pgid)) {
	bst = nextDataPage();
	if (bst != true)
	  return bst;
      }
    }
    
    // Now we are on the correct page.
    
    try{
    	usermid = datapage.firstRecord();
	}
    catch (Exception e) {
      e.printStackTrace();
    }
	

    if (usermid == null)
      {
    	bst = false;
        return bst;
      }
    
    bst = peekNext(nxtmid);
    
    while ((bst == true) && (nxtmid != mid))
      bst = mvNext(nxtmid);
    
    return bst;
  }


}

//In getNext -> modify the getRecord() and nextRecord() functions in heap file.

// Position function in Scan.java is not called anywhere as observed

// firstDataPage in Scan.java is called in init() and Position() 

// nextDataPage 

// peekNext

// mvNext

// pinPage

// unpinPage

// Add TupleSizeException


