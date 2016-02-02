#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

#define MAX_POOLS 50

//The number of current exist buffer pools
int curPools = 0;

//Structure to storage buffer information
/********  Learned from Others  ********/
typedef struct BufferFrame{
    BM_PageHandle ph;
    int fixcount;
    bool dirty;
    int recordPos;
    struct BufferFrame *next;
}BufferFrame;

//Structure to store IO information
/********  Learned from Others  ********/
typedef struct IORecord{
	int numRead;
	int numWrite;
	int pnList[MAX_POOLS];		//list of pageNum int he buffer pool
	bool dList[MAX_POOLS];		//list of page dirty or not in the buffer pool
	int fcList[MAX_POOLS];		//list of pages' fixcount in the buffer pool
	int rPos;			//record current replacement position
	bool freeAccess;			//thread safe lock.
}IORecord;

IORecord record[MAX_POOLS];		//record list, array to record IO manipulation

BufferFrame* initBufferFrame(int RecordPos) //initial the Buffer Frame
{
	BufferFrame *bf;
	bf = (BufferFrame*) malloc (sizeof(BufferFrame));
	bf->fixcount = 0;
	bf->dirty = FALSE;
	bf->recordPos = RecordPos;		//position in the record list
	bf->ph.pageNum = NO_PAGE;      //#define NO_PAGE -1
	bf->ph.data = (char*) malloc (PAGE_SIZE*sizeof(char));
	bf->next = NULL;
	return bf;
}

void freeBufferFrame(BufferFrame *bf)
{
	char *f;
	f=bf->ph.data;
	free(f);
//	printf("free *data\n");
	free(bf);
//	printf("free buffer frame\n");
	return;
}

/************ Buffer Pool Function ************/

RC initBufferPool(BM_BufferPool *const bm,  char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData )
{
	int fd,i = 0;
    
    //Test the file is existed or not
	if((fd=open(pageFileName, O_RDWR)) == -1)
		return RC_FILE_NOT_FOUND;
    
    //Test buffer is enough or not
	if(curPools>=MAX_POOLS)
		return FALSE;
    
    //Initial the attributions of record
	record[curPools].numRead = 0;
	record[curPools].numWrite = 0;
	record[curPools].freeAccess = TRUE;
	record[curPools].rPos = 0;
    
    //Initial buffer pool
	BufferFrame *bf, *tempBf;
	(*bm).pageFile = pageFileName;
	(*bm).numPages = numPages;
	(*bm).strategy = strategy;
	
    //Initial the first page
    bf = (BufferFrame*) malloc (sizeof(BufferFrame));
    bf -> fixcount = 0;
    bf -> dirty = FALSE;
	bf -> recordPos = curPools;
	bf -> ph.pageNum = NO_PAGE; //#define NO_PAGE -1 in buffer_mgr.h
	bf -> ph.data = (char *) malloc (PAGE_SIZE * sizeof(char));
	bf -> next = NULL;
    
    //Compelet the initial operation of buffer pool
	(*bm).mgmtData = bf;

    //Compelet the initial operation of other pages
	for(i=0; i<numPages-1; i++)
    {
        tempBf = (BufferFrame*) malloc (sizeof(BufferFrame));
        tempBf -> fixcount = 0;
        tempBf -> dirty = FALSE;
        tempBf -> recordPos = curPools;
        tempBf -> ph.pageNum = NO_PAGE; //#define NO_PAGE -1 in buffer_mgr.h
        tempBf -> ph.data = (char *) malloc (PAGE_SIZE * sizeof(char));
        tempBf -> next = NULL;
		
        bf->next = tempBf;
		bf = bf->next;
	}
	close(fd);
	curPools++;
    
	return RC_OK;
}

/** destroys a buffer pool. This method should free up all resources associated  **/
/** with buffer pool. For example, it should free the memory allocated for page  **/
/** frames. If the buffer pool contains any dirty pages, then these pages should **/
/** be written back to disk before destroying the pool. It is an error to *********/
/** shutdown a buffer pool that has pinned pages.**********************************/
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	int i;
    char *freeBf;
	BufferFrame *bf, *tempBf;
	//Pointer to address the chain table
    BufferFrame *p;
    SM_FileHandle fileHandle;
	
    //Get the handle of Buffer Pool
    bf = bm->mgmtData;
	
    for(i= 0; i< bm->numPages && bf!=NULL; i++)
	{
		if( bf->fixcount>0 )
            return FALSE;
		bf = bf->next;
	}
    
    //Get the last bf and did corresponding operations
	bf = bm->mgmtData;
	p = bf->next;
	openPageFile(bm->pageFile, &fileHandle);
    
    //printf("numPages: %d\n",bm->numPages);
    //Clean the free resouces used
	for(i= 0; i< bm->numPages && bf!=NULL; i++)
	{
		if( bf->dirty )
        {
			ensureCapacity(bf->ph.pageNum, &fileHandle);
			writeBlock(bf->ph.pageNum, &fileHandle, bf->ph.data);
			record[bf->recordPos].numWrite++;
		}
		
        //Free Buffer Frame
        tempBf = bf;
        freeBf=tempBf->ph.data;
        free(freeBf);
        free(tempBf);
        
		bf=p;
		if(p->next!=NULL)
			p=p->next;
	}
	closePageFile(&fileHandle);
    
	return RC_OK;
}

/****  Causes all dirty pages with fix count 0 from buffer pool to be written to disk ****/
RC forceFlushPool(BM_BufferPool *const bm)
{
	int i;
	BufferFrame *bf, *p;
	SM_FileHandle fileHandle;
	bf = bm->mgmtData;
	p = bf->next;

	//Open the file which will be flush to
    openPageFile(bm->pageFile, &fileHandle);
	for(i= 0; i< bm->numPages; i++)
	{
        //Test the page is dirty or not
		if( bf->dirty==TRUE && bf->fixcount<= 0 )
		{
			ensureCapacity(bf->ph.pageNum+1, &fileHandle);
            writeBlock(bf->ph.pageNum, &fileHandle, bf->ph.data);
			record[bf->recordPos].numWrite++;
			bf->dirty = FALSE;
		}
		bf = bf->next;
	}
	closePageFile(&fileHandle);
	return RC_OK;
}

/************ Page Management Functions ************/

//marks a page as dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	int i,j;
	BufferFrame *bf;
	
	bf = (BufferFrame *)malloc(sizeof(BufferFrame *));
	bf = bm->mgmtData;
	for(i= 0; i< bm->numPages; i++)		//searching for the desired page
	{
		if( bf->ph.pageNum == page->pageNum )		//mark as dirty page.
		{
			bf->dirty = TRUE;
			return RC_OK;
		}
		bf = bf->next;
	}
	
    return FALSE;
}

BufferFrame* checkPageExist(BM_BufferPool *const bm, const PageNumber pageNum)      //check whether the desired page exists in the buffer pool
{																					//if so return the pointer		
    int i;
    BufferFrame *bf;
    bf = bm->mgmtData;
    for(i=0; i<bm->numPages; i++)
    {
        if(bf->ph.pageNum == pageNum)
        {
            return bf;      //if page exist, return bf
        }
        bf = bf->next;
    }
    return bf;      //if page not exist, return bf which points to NULL.
}

/**** Unpins the page page. The pageNum field of page should be used to figure out which page to unpin ****/
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int i;
	BufferFrame *bf, *tempBf;
	//SM_FileHandle fileHandle;
    
    //Check wheter the desired page exists in the buffer pool
    tempBf = bm->mgmtData;
    for(i=0;i<bm->numPages;i++)
    {
        if(tempBf->ph.pageNum == page->pageNum)
            break;
        tempBf = tempBf->next;
    }
    bf = tempBf;
	
    if(bf==NULL)
        return FALSE;

	while(!record[bf->recordPos].freeAccess)
	{
		sleep(0.5);		
	}
    
	record[bf->recordPos].freeAccess = FALSE;
	strcpy(bf->ph.data, page->data);
	record[bf->recordPos].freeAccess = TRUE;
	free(page->data);
	bf->fixcount--;
	
    return RC_OK;
}

/****  write the current content of the page back to the page file on disk  ****/
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int i;
	BufferFrame *bf, *tempBf;
	SM_FileHandle fileHandle;
	
    //Find the bf's right position
    tempBf = bm->mgmtData;
    for(i=0;i<bm->numPages;i++)
    {
        if(tempBf->ph.pageNum == page->pageNum)
            break;
        tempBf = tempBf->next;
    }
    bf = tempBf;
    
	if(bf==NULL)
		return FALSE;
	
    while(!record[bf->recordPos].freeAccess)
        sleep(0.5);
    
	record[bf->recordPos].freeAccess = FALSE;
	strcpy(bf->ph.data, page->data);
	record[bf->recordPos].freeAccess = TRUE;
	openPageFile(bm->pageFile, &fileHandle);
	
    ensureCapacity(page->pageNum, &fileHandle);
	writeBlock(bf->ph.pageNum, &fileHandle, bf->ph.data);
	
    record[bf->recordPos].numWrite++;
	closePageFile(&fileHandle);
	
    return RC_OK;
}


BufferFrame* addPagetoPool(BM_BufferPool *const bm, const PageNumber pageNum)
{
    int i, pos, k;
	BufferFrame *bf, *p, *l, *tempBf, *q;		//l,p is pointer to help to delete the old node
	SM_FileHandle fh;

	p = bm->mgmtData;
	bf = initBufferFrame(p->recordPos);     //create new buffer frame node
    openPageFile(bm->pageFile, &fh);            //open page file
    ensureCapacity(pageNum+1, &fh);             //ensure capacity
    readBlock(pageNum, &fh, bf->ph.data);       //read desired page into buffer frame
    record[p->recordPos].numRead++;     //update record info
	bf->ph.pageNum = pageNum;                   //record pageNum in buffer frame
    closePageFile(&fh); 				//close page file

	switch(bm->strategy)
	{
		case RS_FIFO:					//the replacement strategy FIFO
			pos = record[p->recordPos].rPos;
			for(i=0; i<pos; i++)		//located to the position to be replaced
			{
				l = p;
				p = p->next;
			}
			if(p->fixcount>0)
			{
				for(i=0; p->fixcount>0&&i<bm->numPages; i++)
				{
					if(p->next == NULL)
					{
						p = bm->mgmtData;
					}					
					else
					{
						p = p->next;
					}
				}
				if(i==bm->numPages)
				{
					printf("All pages in the buffer pool are under used, pin failed.\n");
					return NULL;
				}
			}
			//Find the correct previous node of p
            tempBf = bm->mgmtData;
            q = NULL;
            
            for(k=0;k<bm->numPages;k++)
            {
                if(tempBf->ph.pageNum == p->ph.pageNum)
                    break;
                q = tempBf;
                tempBf = tempBf->next;
            }
            l = q;
            
            if(p->dirty)
			{
				openPageFile(bm->pageFile, &fh);
				ensureCapacity(p->ph.pageNum+1, &fh);
				writeBlock(p->ph.pageNum, &fh, p->ph.data);
				record[p->recordPos].numWrite++;       //update record info
				closePageFile(&fh);
            }
			if(l == NULL)
			{
				bf->next = p->next;
				bm->mgmtData = bf;
			}
			else
			{
				bf->next = p->next;
				l->next = bf;
			}
			record[p->recordPos].rPos = (record[p->recordPos].rPos+1) % bm->numPages;
			freeBufferFrame(p);
			break;

		case RS_LRU:						//replacement strategy:LRU
			p = bm->mgmtData; 
			for(i=0; i<bm->numPages-1; i++) //search to the last node of the chain table and delete it.
            {
                l=p;
                p=p->next;
            }		//after the search, p points to the last node of the chain table, l points to the node before p
            if(p->fixcount>0)
            {
                printf("Buffer pool is full and under using. Failed to add page.\n ");
                return NULL;
            }

			bf->next = bm->mgmtData;			//insert the new buffer frame node into the chain table
			bm->mgmtData = bf;

			if(p->dirty)               //write the page back to the file if it is dirty
			{
	         	openPageFile(bm->pageFile, &fh);
		        ensureCapacity(p->ph.pageNum+1, &fh);
			    writeBlock(p->ph.pageNum, &fh, p->ph.data);
				record[p->recordPos].numWrite++;       //update record info
		        closePageFile(&fh);
		    }			

			freeBufferFrame(p);			//free the last node
			l->next = NULL;
			break;
	}
	return bf;
}

//pins the page with page number pageNum. The buffer manager is responsible to set the pageNum field of the page handle passed to the method. Similarly, the data field should point to the page frame the page is stored in
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    int i;
	BufferFrame *bf, *tempBf, *p, *q;
	
    //Check the page is existed or not
    
    tempBf = bm->mgmtData;
    for(i=0; i<bm->numPages; i++)
    {
        if(tempBf->ph.pageNum == pageNum)
            break;
        tempBf = tempBf->next;
    }
    bf = tempBf;
    
	if(bf==NULL)
    //If the page isn't exist, add it in the buffer pool
	{
        bf=addPagetoPool(bm, pageNum);
        
        if(bf == NULL)
            return FALSE;
    }
    
    //Put the nodes in the correct position according to the strategy
    switch(bm->strategy)
    {
        case RS_FIFO:
            break;
        
        case RS_LRU:
            tempBf = bm->mgmtData;
            q = NULL;
            
            for(i=0; i<bm->numPages;i++)
            {
                if(tempBf->ph.pageNum == bf->ph.pageNum)
                    break;
                q=tempBf;
                tempBf=tempBf->next;
            }
            p = q;
            
            p->next = bf->next;
            bf->next = bm->mgmtData;
            bm->mgmtData = bf;
            break;
    }
    bf->fixcount++;
    
    page->data = (char*) malloc (sizeof(char)*PAGE_SIZE);
    
    while(!record[bf->recordPos].freeAccess)
        sleep(0.5);
    
    record[bf->recordPos].freeAccess = FALSE;
    strcpy(page->data, bf->ph.data);
    record[bf->recordPos].freeAccess = TRUE;
    page->pageNum = bf->ph.pageNum;
    
    return RC_OK;
}

/************ Statistics Functions ************/

//function returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame. An empty page frame is represented using the constant NO_PAGE.
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
	int num = bm->numPages;
	int i, pos;
	BufferFrame *bf;
	bf = bm->mgmtData;
	pos = bf->recordPos;
	for(i=0; i<num; i++)
	{
		record[pos].pnList[i] = bf->ph.pageNum;
		bf = bf->next;
	}
	return record[pos].pnList;
}

//returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty. Empty page frames are considered as clean.
bool *getDirtyFlags(BM_BufferPool *const bm)
{
	int num = bm->numPages;
	int i, pos;
	BufferFrame *bf;
	bf = bm->mgmtData;
	pos = bf->recordPos;
	for(i=0; i<num; i++)
	{
		record[pos].dList[i] = bf->dirty;
		bf = bf->next;
	}

    return record[pos].dList;
}

//returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame. Return 0 for empty page frames.
int *getFixCounts(BM_BufferPool *const bm)
{
	int num = bm->numPages;
	int i, pos;
	BufferFrame *bf;
	bf = bm->mgmtData;
	pos = bf->recordPos;
	for(i=0; i<num; i++)
	{
		record[pos].fcList[i] = bf->fixcount;
		bf = bf->next;
	}
	return record[pos].fcList;
}

//returns the number of pages that have been read from disk since a buffer pool has been initialized. You code is responsible to initializing this statistic at pool creating time and update whenever a page is read from the page file into a page frame.
int getNumReadIO(BM_BufferPool *const bm)
{
	int num;
	BufferFrame *bf;
	bf = bm->mgmtData;
	num = record[bf->recordPos].numRead;
	return num;
}

//returns the number of pages written to the page file since the buffer pool has been initialized.
int getNumWriteIO(BM_BufferPool *const bm)
{
	int num;
	BufferFrame *bf;
	bf = bm->mgmtData;
	num = record[bf->recordPos].numWrite;
	return num;
}
