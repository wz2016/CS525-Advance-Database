#ifndef CLOCK_H
#define CLOCK_H

#include "dt.h"
#include "dberror.h"
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <time.h>

typedef struct mgmtDataCLOCK {
    int fix;
    bool dirty;
    clock_t time; //record time for a page used
    BM_PageHandle *pagehandle;
} mgmtDataCLOCK;

typedef struct CLOCK_strategyData{
    int getCurrentPos;
}CLOCK_strategyData;

/************************************************************
 *                    interface                             *
 ************************************************************/
extern RC initmgmtdataCLOCK (BM_BufferPool * bm);
//create array of mgmtdataCLOCK correspond to the pagenum pointing by mgmtdata

int CLOCK_findTheEariestUnfixedNode(BM_BufferPool * const bm);

RC CLOCK_makeDirty(BM_BufferPool *const bm, BM_PageHandle *const page);

RC CLOCK_resetDirty(BM_BufferPool *const bm, BM_PageHandle *const page);

RC CLOCK_unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page);

RC CLOCK_pinPage (BM_BufferPool *const bm,BM_PageHandle *const page, const PageNumber pageNum);

int CLOCK_findByPageBumber(BM_BufferPool *const bm, BM_PageHandle *const page);

int *CLOCK_getFixCounts(BM_BufferPool * const bm);
bool *CLOCK_getDirtyFlags(BM_BufferPool * const bm);
PageNumber *CLOCK_getFrameContents(BM_BufferPool * const bm);

#endif
