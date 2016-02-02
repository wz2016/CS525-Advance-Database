#include <stdlib.h>
#include <string.h>

#include "FIFO.h"
#include "dberror.h"
#include "buffer_mgr.h"

//if the page recorded by the buffer pool, then return the index
int FIFO_findByPageNumber(BM_BufferPool *const bm, int pageNum){
    mgmtDataFIFO * nodes = (mgmtDataFIFO * )bm->mgmtData;
    int i;
    for (i = 0; i < bm->numPages; i++) {
        if(nodes[i].pagehandle->pageNum == pageNum){
            return i;
        }
    }
    return -1;
}

//init the mgmtDate into mgmtDataFIFO (one kind of page frame)
RC initmgmtdataFIFO(BM_BufferPool * const bm){
    mgmtDataFIFO * nodes = (mgmtDataFIFO *)malloc(bm->numPages*sizeof(struct mgmtDataFIFO));
    int i;
    for(i=0;i<bm->numPages;i++){
        nodes[i].fix = 0;
        nodes[i].dirty = false;
        nodes[i].time = 0;
        nodes[i].pagehandle = malloc(sizeof(struct BM_PageHandle));
        nodes[i].pagehandle->pageNum = NO_PAGE;
        nodes[i].pagehandle->data = (char *)malloc(PAGE_SIZE*sizeof(char));
    }
    bm->mgmtData = nodes;
    return RC_OK;
}

RC FIFO_makeDirty(BM_BufferPool *const bm, BM_PageHandle *const page){
    int i = FIFO_findByPageNumber(bm, page->pageNum);
    if(i<0){
        return RC_BM_PAGE_NOT_BUFFERED;
    }
    mgmtDataFIFO * nodes = (mgmtDataFIFO * )bm->mgmtData;
    nodes[i].dirty = true;
    
    return RC_OK;
}
//set the dirty to false in the mgmtDataFIFO
RC FIFO_resetDirty(BM_BufferPool *const bm, BM_PageHandle *const page){
    int i = FIFO_findByPageNumber(bm, page->pageNum);
    if(i<0){
        return RC_BM_PAGE_NOT_BUFFERED;
    }
    mgmtDataFIFO * nodes = (mgmtDataFIFO * )bm->mgmtData;
    nodes[i].dirty = false;
    //node.time = 0;
    return RC_OK;
}
//adding one to the fix for the pin page in the buffer pool
//And adding one to each time of mgmtDataFIFO in the buffer pool
RC FIFO_pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, PageNumber pageNum){
    //printf("pin: %d\n", pageNum);
    int i;
    int ind = FIFO_findByPageNumber(bm, pageNum);
    //get the system time at pinPage;
    clock_t c = clock();
    
    mgmtDataFIFO * nodes = (mgmtDataFIFO *)bm->mgmtData;
    
    RC _rc;
    SM_FileHandle fHandle;
    //if the page is in buffer, return the pagehandle
    if (ind >= 0){
        page->pageNum = pageNum;
        page->data = nodes[ind].pagehandle->data;
        
        nodes[ind].fix++;

//        nodes[ind].time = (double)seconds;

        //nodes[ind].time = c;

        //printf("page found in buffer: %d\n", ind);
        return RC_OK;
    }
    
    //if there are free page in the buffer
    for(i=0; i<bm->numPages; i++){
        if ( nodes[i].pagehandle->pageNum == NO_PAGE){
            //printf("%dth page is free\n", i);
            nodes[i].pagehandle->pageNum = pageNum;
            free(nodes[i].pagehandle->data);
            nodes[i].pagehandle->data = (char *)calloc(1, PAGE_SIZE);
            _rc = openPageFile(bm->pageFile, &fHandle);
            if(_rc != RC_OK){
                return _rc;
            }
            _rc = readBlock(pageNum, &fHandle, nodes[i].pagehandle->data);
            closePageFile(&fHandle);
            if(_rc == RC_OK){
                //printf("page # %d is in disk\n", pageNum);
                bm->numOfRead++;
            }
            nodes[i].fix++;
            nodes[i].time = c;
            nodes[i].dirty = false;
            
            page->pageNum = pageNum;
            page->data = nodes[i].pagehandle->data;
            //printf("pinpage succeed, %dth page become page# %d\n", i, page->pageNum);
            return RC_OK;
        }
    }
    //if all buffers have data
    i = FIFO_findTheEariestUnfixedNode(bm);
    if (i == -1){
        //all buffers are full and occupied
        return RC_BM_ALL_PAGE_PINNED;
    }
    else{
            //write the content to disk
        if(nodes[i].dirty){
            _rc = forcePage(bm,nodes[i].pagehandle);
            //bm->numOfWrite++;
        }
            nodes[i].pagehandle->pageNum = pageNum;
            free(nodes[i].pagehandle->data);
            nodes[i].pagehandle->data = (char *)calloc(1, PAGE_SIZE);
            
            _rc = openPageFile(bm->pageFile, &fHandle);
            if(_rc != RC_OK){
                return _rc;
            }
            _rc = readBlock(pageNum, &fHandle, nodes[i].pagehandle->data);
            closePageFile(&fHandle);
            if(_rc == RC_OK){
                bm->numOfRead++;
            }
            nodes[i].fix++;
            nodes[i].time = c;
            nodes[i].dirty = false;
//            nodes[i].time  = (double)seconds;
            page->pageNum = pageNum;
            page->data = nodes[i].pagehandle->data;
            
            return RC_OK;
    }

}
//find the earliest
int FIFO_findTheEariestUnfixedNode(BM_BufferPool * const bm){
    mgmtDataFIFO * nodes = (mgmtDataFIFO * )bm->mgmtData;
    int earliestIndex = -1;
    clock_t earliestTime = -1; //earliesTime
	int i;
    for(i=0; i<bm->numPages; i++){
        if ( nodes[i].fix == 0){
            if(earliestIndex == -1){
                earliestTime = nodes[i].time;
                earliestIndex = i;
                //printf("new ind is: %d\n", i);
            }
            //printf("%d - %d\n", (int)earliestTime, (int)nodes[i].time);
            if (((int)earliestTime-(int)nodes[i].time) > 0){
                earliestTime = nodes[i].time;
                earliestIndex = i;
            }
        }
    }
    return earliestIndex;
}
//reduce one to the fix
RC FIFO_unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page){
    int i = FIFO_findByPageNumber(bm, page->pageNum);
    mgmtDataFIFO * nodes = (mgmtDataFIFO * )bm->mgmtData;
    
    if(i<0){
        return RC_BM_PAGE_NOT_BUFFERED;
    }
    nodes[i].fix--;
    //printf("unpin: %d[fix becomes %d]\n", page->pageNum, nodes[i].fix);
    return RC_OK;
}


int *FIFO_getFixCounts(BM_BufferPool * const bm) {
    mgmtDataFIFO *nodes = (mgmtDataFIFO *)bm->mgmtData;
    int *r = (int *) malloc(sizeof(int) * bm->numPages);

    int i;
    for (i = 0; i < bm->numPages; i++) {
        if (nodes[i].pagehandle) {
            r[i] = nodes[i].fix;
        } else {
            r[i] = 0;
        }
    }

	free(r);
    return r;
}

PageNumber *FIFO_getFrameContents(BM_BufferPool * const bm) {
    mgmtDataFIFO *nodes = (mgmtDataFIFO *)bm->mgmtData;
    PageNumber *r = (PageNumber *) malloc(sizeof(PageNumber) * bm->numPages);

    int i;
    for (i = 0; i < bm->numPages; i++) {
        if (nodes[i].pagehandle) {
            r[i] = nodes[i].pagehandle->pageNum;
        } else {
            r[i] = NO_PAGE;
        }
    }
	free(r);
    return r;

}

bool *FIFO_getDirtyFlags(BM_BufferPool * const bm) {

    mgmtDataFIFO *nodes = (mgmtDataFIFO *)bm->mgmtData;
    bool *r = (bool *) malloc(sizeof(bool) * bm->numPages);

    int i;
    for (i = 0; i < bm->numPages; i++) {
        if (nodes[i].pagehandle) {
            r[i] = nodes[i].dirty;
        } else {
            r[i] = false;
        }
    }
	free(r);
    return r;
}
