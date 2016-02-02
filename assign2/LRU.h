#ifndef LRU_H
#define LRU_H

#include "dt.h"
#include "dberror.h"
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <time.h>

typedef struct mgmtDataLRU {
    int fix;
    bool dirty;
    clock_t time; //record time for a page used
    BM_PageHandle *pagehandle;
} mgmtDataLRU;

typedef struct LRU_strategyData{
    int getCurrentPos;
}LRU_strategyData;

/************************************************************
 *                    interface                             *
 ************************************************************/
extern RC initmgmtdataLRU (BM_BufferPool * bm);
//create array of mgmtdataLRU correspond to the pagenum pointing by mgmtdata

int LRU_findTheEariestUnfixedNode(BM_BufferPool * const bm);

RC LRU_makeDirty(BM_BufferPool *const bm, BM_PageHandle *const page);

RC LRU_resetDirty(BM_BufferPool *const bm, BM_PageHandle *const page);

RC LRU_unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page);

RC LRU_pinPage (BM_BufferPool *const bm,BM_PageHandle *const page, const PageNumber pageNum);

int LRU_findByPageBumber(BM_BufferPool *const bm, BM_PageHandle *const page);

int *LRU_getFixCounts(BM_BufferPool * const bm);
bool *LRU_getDirtyFlags(BM_BufferPool * const bm);
PageNumber *LRU_getFrameContents(BM_BufferPool * const bm);

#endif
