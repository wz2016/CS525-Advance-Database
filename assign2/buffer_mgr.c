#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

#include "dberror.h"
#include "dt.h"
#include "buffer_mgr_stat.h"
#include "FIFO.h"
#include "LRU.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "CLOCK.h"

//The number of current exist buffer pools

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData){
    
    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->numOfRead = 0;
    bm->numOfWrite = 0;
    bm->currentPos_In = 0;
    bm->currentPos_Out = 0;
    
    if(strategy == RS_FIFO){
        initmgmtdataFIFO(bm);
    }
    else if(strategy == RS_LRU){
        initmgmtdataLRU(bm);
    }
    else if(strategy == RS_CLOCK){
        initmgmtdataCLOCK(bm);
    }
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    int i;
    for(i=0;i<bm->numPages;i++){
        if(bm->strategy == RS_FIFO){
            forcePage(bm, ((mgmtDataFIFO *)bm->mgmtData)[i].pagehandle);
            free(((mgmtDataFIFO *)bm->mgmtData)[i].pagehandle->data);
            free(((mgmtDataFIFO *)bm->mgmtData)[i].pagehandle);
        } else if (bm->strategy == RS_LRU){
            forcePage(bm, ((mgmtDataLRU *)bm->mgmtData)[i].pagehandle);
            free(((mgmtDataLRU *)bm->mgmtData)[i].pagehandle->data);
            free(((mgmtDataLRU *)bm->mgmtData)[i].pagehandle);
        }
        else if (bm->strategy == RS_CLOCK){
            forcePage(bm, ((mgmtDataCLOCK *)bm->mgmtData)[i].pagehandle);
            free(((mgmtDataCLOCK *)bm->mgmtData)[i].pagehandle->data);
            free(((mgmtDataCLOCK *)bm->mgmtData)[i].pagehandle);
        }
    }
    
    free(bm->mgmtData);
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
    int i;
    int _rc;
    for(i=0;i<bm->numPages;i++){

        switch (bm->strategy) {
            case RS_FIFO:
                if(((mgmtDataFIFO *)(bm->mgmtData))[i].dirty == true){
                    forcePage(bm, ((mgmtDataFIFO *)(bm->mgmtData))[i].pagehandle);
                }
                break;
                
            case RS_LRU:
                if(((mgmtDataLRU *)(bm->mgmtData))[i].dirty == true){
                    forcePage(bm, ((mgmtDataLRU *)bm->mgmtData)[i].pagehandle);
                }
                break;
            case RS_CLOCK:
            if(((mgmtDataCLOCK *)(bm->mgmtData))[i].dirty == true){
                forcePage(bm, ((mgmtDataLRU *)bm->mgmtData)[i].pagehandle);
            }
            break;
            default:
                break;
        }
        
    }
    return RC_OK;
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    if (!bm || !page){
        return RC_READ_NON_EXISTING_PAGE;
    }
    int _rc;
    //according to the strategy of BM_bufferPool, to selete the kinds of mgmtdata
    switch (bm->strategy) {
        case RS_FIFO:
            return _rc = FIFO_makeDirty(bm, page);
    
        case RS_LRU:
            return _rc = LRU_makeDirty(bm, page);
            
        case RS_CLOCK:
            return _rc = CLOCK_makeDirty(bm, page);
            
        case RS_LFU:
            
        default:
            break;
    }
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    if (!bm || !page){
        return RC_READ_NON_EXISTING_PAGE;
    }
    int _rc;
    //according to the strategy of BM_bufferPool, to selete the kinds of mgmtdata
    switch (bm->strategy) {
        case RS_FIFO:
            return _rc = FIFO_unpinPage(bm, page);
            
        case RS_LRU:
            return _rc = LRU_unpinPage(bm, page);
            
        case RS_CLOCK:
            return _rc = CLOCK_unpinPage(bm, page);
        case RS_LFU:
            
        default:
            break;
    }
    return RC_OK;
}


//write the current content of the page back to the page file on disk
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    
    if (!bm || !page){
        return RC_READ_NON_EXISTING_PAGE;
    }
    SM_FileHandle fHandle;
    int _rc;
    
        _rc = openPageFile(bm->pageFile, &fHandle);
    if(_rc != RC_OK){
        return _rc;
    }
        
        _rc = writeBlock(page->pageNum, &fHandle, page->data);
    if(_rc != RC_OK){
        return _rc;
    }
    bm->numOfWrite++;
    
        closePageFile(&fHandle);
//reset the dirty of the page frame in the buffer pool.
    switch (bm->strategy) {
        case RS_FIFO:
            FIFO_resetDirty(bm,page);
            break;
            
        case RS_LRU:
            LRU_resetDirty(bm,page);
            break;
        
        case RS_CLOCK:
            CLOCK_resetDirty(bm,page);
            break;
        
        default:
            break;
    }
    return RC_OK;
    
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum){
    if (!bm || !page){
        return RC_READ_NON_EXISTING_PAGE;
    }
    int _rc;
    switch (bm->strategy) {
        case RS_FIFO:
            _rc = FIFO_pinPage(bm,page,pageNum);
            return _rc;
        case RS_LRU:
            _rc = LRU_pinPage(bm,page,pageNum);
            return _rc;
            
        case RS_CLOCK:
            _rc = CLOCK_pinPage(bm,page,pageNum);
            return _rc;
        
        case RS_LFU:
            
        default:
            break;
    }
    return RC_BM_INVALID_STRATEGY;
}

/************ Statistics Functions ************/

//returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame. An empty page frame is represented using the constant NO_PAGE.
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    switch (bm->strategy) {
        case RS_FIFO:
            return FIFO_getFrameContents(bm);
        case RS_LRU:
            return LRU_getFrameContents(bm);
        case RS_CLOCK:
            return CLOCK_getFrameContents(bm);
        default:
            break;
    }

    return NULL;
}

//returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty. Empty page frames are considered as clean.
bool *getDirtyFlags(BM_BufferPool *const bm)
{

    switch (bm->strategy) {
        case RS_FIFO:
            return FIFO_getDirtyFlags(bm);
        case RS_LRU:
            return LRU_getDirtyFlags(bm);
        case RS_CLOCK:
            return CLOCK_getDirtyFlags(bm);
        default:
            break;
    }

    return NULL;
}

//returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame. Return 0 for empty page frames.
int *getFixCounts(BM_BufferPool *const bm)
{

    switch (bm->strategy) {
        case RS_FIFO:
            return FIFO_getFixCounts(bm);
        case RS_LRU:
            return LRU_getFixCounts(bm);
        case RS_CLOCK:
            return CLOCK_getFixCounts(bm);
        default:
            break;
    }
    return NULL;
}

//returns the number of pages that have been read from disk since a buffer pool has been initialized. You code is responsible to initializing this statistic at pool creating time and update whenever a page is read from the page file into a page frame.
int getNumReadIO(BM_BufferPool *const bm)
{
    if (!bm)
        return -1;
    else
	return bm->numOfRead;
}

//returns the number of pages written to the page file since the buffer pool has been initialized.
int getNumWriteIO(BM_BufferPool *const bm)
{
    if (!bm)
        return -1;
    else
	return bm->numOfWrite;
}
