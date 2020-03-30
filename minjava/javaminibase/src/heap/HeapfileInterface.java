package heap;

import global.*;
import BigT.Map;

import java.io.*;
import java.util.*;

public interface HeapfileInterface {
    public PageId getFirstDirPageId();
    public MID insertRecordMap(byte[] recPtr) throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException, FieldNumberOutOfBoundException, Exception;
    public boolean updateRecordMap(MID mid, Map newmap)
                throws InvalidSlotNumberException,
                InvalidUpdateException,
                InvalidTupleSizeException,
                HFException,
                HFDiskMgrException,
                HFBufMgrException,
                Exception;
    public boolean deleteRecordMap(MID mid)
                        throws InvalidSlotNumberException,
                        InvalidTupleSizeException,
                        HFException,
                        HFBufMgrException,
                        HFDiskMgrException,
                        Exception;
    public int getRecCntMap()
                throws InvalidSlotNumberException,
                InvalidTupleSizeException,
                HFDiskMgrException,
                HFBufMgrException,
                IOException;
    public void deleteFileMap()
                throws InvalidSlotNumberException,
                FileAlreadyDeletedException,
                InvalidTupleSizeException,
                HFBufMgrException,
                HFDiskMgrException,
                IOException;
}