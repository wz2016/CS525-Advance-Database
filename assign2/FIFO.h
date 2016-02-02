#ifndef FIFO_H
#define FIFO_H

#include "dt.h"
#include "dberror.h"
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <time.h>

typedef struct mgmtDataFIFO {
    int fix;
    bool dirty;
    clock_t time; //record time for a page created
    
    BM_PageHandle *pagehandle;
} mgmtDataFIFO;

typedef struct FIFO_strategyData{
    int getCurrentPos;
}FIFO_strategyData;
/************************************************************
 *                    interface                             *
 ************************************************************/
extern RC initmgmtdataFIFO (BM_BufferPool * bm);
//create array of mgmtdataFIFO correspond to the pagenum pointing by mgmtdata
//extern RC requestFIFO(BM_BufferPool * bm, PageNumber pageNum, char* data);
//substitute corresponding pagehandle with new frame of data according to FIFO

int FIFO_findTheEariestUnfixedNode(BM_BufferPool * const bm);

RC FIFO_makeDirty(BM_BufferPool *const bm, BM_PageHandle *const page);

RC FIFO_resetDirty(BM_BufferPool *const bm, BM_PageHandle *const page);

RC FIFO_unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page);

RC FIFO_pinPage (BM_BufferPool *const bm,BM_PageHandle *const page, const PageNumber pageNum);

int FIFO_findByPageBumber(BM_BufferPool *const bm, BM_PageHandle *const page);

int *FIFO_getFixCounts(BM_BufferPool * const bm);
bool *FIFO_getDirtyFlags(BM_BufferPool * const bm);
PageNumber *FIFO_getFrameContents(BM_BufferPool * const bm);
#endif
