package heap;

import global.*;

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
}