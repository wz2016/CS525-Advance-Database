#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>

#include "dberror.h"
#include "storage_mgr.h"

/* manipulating page files */

int fileDescriptor[10]; //Store file descriptor for each open file, maximun 10 open files
int i=0;

//initiate storage manager, print welcome message
void initStorageManager(void)
{
    //printf("Welcom to CS525 Storage Manager!\n");
}


//This method create a new file with fileName parameter.
//The new file size is one page with '\0' bytes.
RC createPageFile (char *fileName)
{
    FILE * pFile;
    pFile = fopen(fileName, "w");
    
    if (pFile == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
    
    //Fill the page with 0
    char fillBuff[PAGE_SIZE] = { 0 };
    
    if (fwrite(fillBuff, sizeof(fillBuff), 1, pFile) == -1)
        return RC_WRITE_FAILED;
    
    fclose(pFile);
    
    return RC_OK;
    
}

//This method open an existing page file.
//If succesfully open a file, then the file handler should be changed to the information about the opened file.
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    struct stat buf;
    int numOfPages;
    FILE * pFile;
    
    if ((fileDescriptor[i]=open(fileName,O_RDWR))==-1)
        return RC_FILE_NOT_FOUND; // File not found
    
    fstat(fileDescriptor[i], &buf);//return information about a file
    
    fHandle->fileName = fileName; //set the fileName attribute of fHandle to be the current fileName
    
    //calculate the total num of pages
    if(buf.st_size == 0)
        numOfPages = 1;
    else
    {
        if(buf.st_size % PAGE_SIZE)
            numOfPages = buf.st_size/PAGE_SIZE + 1;
        else
            numOfPages = buf.st_size/PAGE_SIZE;
    }
    
    fHandle->totalNumPages = numOfPages; // set the totalNumPages attribute of fHandle to the calculated page size of the current file
    fHandle->curPagePos = 0;//freshly opened file's page position should be 0
    fHandle->mgmtInfo = fileDescriptor + i; //get mgmtInfo pointer to the ith index in the fileDescriptor
    
    //get the new i
    i=(i+1)%10;
    
    return RC_OK;
}

//Close an open page file.
RC closePageFile (SM_FileHandle *fHandle)	
{
    if(fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
    
    int * fd;
    fd = (int *)fHandle->mgmtInfo;
    
    close(*fd);
    
    //clear related file information
    fHandle->fileName = NULL;
    fHandle->curPagePos = -1;
    fHandle->totalNumPages = -1;
	   
    return RC_OK;
}

//This method destroys a file.
RC destroyPageFile (char *fileName)
{
    if(remove(fileName) != 0)
        return RC_FILE_NOT_FOUND;
    
    return RC_OK;
}

//This method reads block from a file and stores its content to memory
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    
    int totNumP = fHandle -> totalNumPages;
    
    //If the file has less than pageNum pages, the method should return RC_READ_NON_EXISTING_PAGE.
    if ( totNumP -1 < pageNum || pageNum < 0)
        return RC_READ_NON_EXISTING_PAGE;
        
    if(lseek(*(int *)fHandle -> mgmtInfo, pageNum*PAGE_SIZE, SEEK_SET) == -1)
            return RC_READ_NON_EXISTING_PAGE;
    
    if(read(*(int *)fHandle -> mgmtInfo, memPage, PAGE_SIZE) == -1)
            return RC_READ_NON_EXISTING_PAGE;
        
    fHandle -> curPagePos = pageNum;
    return RC_OK;
}

//Return the current page position in a file
int getBlockPos (SM_FileHandle *fHandle){
    return fHandle -> curPagePos;
}

//Read the first page in file
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

    if(lseek(*(int *)fHandle->mgmtInfo, 0, SEEK_SET) == -1)
		return RC_READ_NON_EXISTING_PAGE;
    
	if(read(*(int *)fHandle->mgmtInfo, memPage, PAGE_SIZE) == -1)
		return RC_READ_NON_EXISTING_PAGE;
    
    fHandle -> curPagePos = 0;
    
    return RC_OK;
}

//Read the previous page relative to the curPagePos of the file
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
    //If the user tries to read a block before the first page, the method return RC_READ_NON_EXISTING_PAGE
    if(lseek(*(int *)fHandle -> mgmtInfo, (fHandle -> curPagePos - 1)*PAGE_SIZE, SEEK_SET) == -1)
        return RC_READ_NON_EXISTING_PAGE;
    
    if(read(*(int *)fHandle -> mgmtInfo, memPage, PAGE_SIZE) == -1)
        return RC_READ_NON_EXISTING_PAGE;
    
    fHandle -> curPagePos = fHandle -> curPagePos - 1;
    
    return RC_OK;
    
    
}

//Read the current page
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
    if(lseek(*(int *)fHandle -> mgmtInfo, (fHandle -> curPagePos)*PAGE_SIZE, SEEK_SET) == -1)
        return RC_READ_NON_EXISTING_PAGE;
    
    if(read(*(int *)fHandle -> mgmtInfo, memPage, PAGE_SIZE) == -1)
        return RC_READ_NON_EXISTING_PAGE;
    
    return RC_OK;
    
}

//Read the next page relative to the curPagePos of the file
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    //If the user tries to read a block after the last page, the method return RC_READ_NON_EXISTING_PAGE
    if(lseek(*(int *)fHandle -> mgmtInfo, (fHandle -> curPagePos + 1)*PAGE_SIZE, SEEK_SET) == -1)
        return RC_READ_NON_EXISTING_PAGE;
    
    if(read(*(int *)fHandle -> mgmtInfo, memPage, PAGE_SIZE) == -1)
        return RC_READ_NON_EXISTING_PAGE;
    
    fHandle -> curPagePos = fHandle -> curPagePos + 1;
    
    return RC_OK;

}

//Read the last page
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

    //printf("totalNUmber is %d:",fHandle -> totalNumPages);
    if(lseek(*(int *)fHandle -> mgmtInfo, (fHandle -> totalNumPages - 1)*PAGE_SIZE, SEEK_SET) == -1)
        return RC_READ_NON_EXISTING_PAGE;
    
    if(read(*(int *)fHandle -> mgmtInfo, memPage, PAGE_SIZE) == -1)
        return RC_READ_NON_EXISTING_PAGE;
    
    fHandle -> curPagePos = fHandle -> totalNumPages-1;
    
    return RC_OK;
}

//write the pageNumth block
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    ensureCapacity(pageNum, fHandle);//make sure the pagenumberth page are exist
    lseek(*(int*)fHandle->mgmtInfo, pageNum*PAGE_SIZE, SEEK_SET);//set offest
    if(write(*(int*)fHandle->mgmtInfo, memPage, PAGE_SIZE) == -1){
        return RC_WRITE_FAILED;
    }
    return RC_OK;
}

//write current block
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    lseek(*(int*)fHandle->mgmtInfo, (fHandle->curPagePos)*PAGE_SIZE, SEEK_SET);//set offest
    if(write(*(int*)fHandle->mgmtInfo, memPage,  PAGE_SIZE) == -1){
        return RC_WRITE_FAILED;
    }
    return RC_OK;
}

//append empty block to the end
RC appendEmptyBlock (SM_FileHandle *fHandle){
    FILE *pFile=fopen(fHandle->fileName, "a");
    if(pFile==NULL) {
        return RC_FILE_NOT_FOUND;
    }
    char initarr[PAGE_SIZE] = {0};//empty page buffer
    if(fwrite(initarr, sizeof(initarr), 1, pFile) == -1){//append to the end of page file
        return RC_WRITE_FAILED;
    }
    fHandle->totalNumPages = fHandle->totalNumPages + 1;//update totalpagenumber
    //printf("totalNumber is %d:",fHandle -> totalNumPages);
    fclose(pFile);
    return RC_OK;
}

//append the file and increase totoal number of pages if needed
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    int rt = 0;
    int i;
    if(fHandle->totalNumPages<numberOfPages+1){
        for(i=0; i<numberOfPages+1-fHandle->totalNumPages; i++){
            if((rt=appendEmptyBlock(fHandle)) != 0){
                return rt;
            }
        }
    }
    return RC_OK;
}
