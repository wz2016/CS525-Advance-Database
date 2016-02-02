2015 Fall CS525 Programming Assignment 2 - Buffer Manager
Due Date: 10/06/2015

Group Member:
WeiLun Zhao | A20329942 | wzhao32@hawk.iit.edu
Su Feng | A20338748 | sfeng14@hawk.iit.edu
Yuchen Tang | A20352138 | ytang34@hawk.iit.edu

Description：
The goal of the assignment is implement a buffer manager - fixing number of pages in memory that represent pages from a page file managed by the storage manager implemented in assignment 1. We implement methods in the buffer_mgr.c which includes two strategies ( FIFO, LRU) to manage the buffer pool, when client sends request to the buffer manager.

Files Include：
README.txt
Makefile		- Building code
buffer_mgr_stat.c 	- Not Modified
buffer_mgr_stat.h 	- Not Modified
buffer_mgr.c 		- Buffer manager functions
buffer_mgr.h 		- Buffer manager functions header (BM_BufferPool changed)
dberror.c 		- Not Modified
dberror.h 		- Not Modified
dt.h 			- Not Modified
FIFO.h 			- FIFO strategy functions header
FIFO.c 			- FIFO strategy functions
CLOCK.h			- CLOCK strategy functions header
CLOCK.c			- CLOCK strategy functions
LRU.h 			- LRU strategy functions header
LRU.c 			- LRU strategy functions
storage_mgr.c 		- Storage manager function as assignment 1
storage_mgr.h 		- Not Modified
test_assign2_1.c 	- Not Modified
test_helper.h 		- Not Modified

How to compile and test:
1.Open terminal and go to the assign2 folder
2.Type in: make (Makefile should be excucted correctly and output is shown below.)
3.Type in: ./buffer_mgr (test_assign2_1.c should run and test against storage_mgr.c with no error.)
4.Type in: make clean (Files other than source files will get removed.)

Buffer_mgr.c
part 1: Buffer Manager Interface Pool Handling
	a. initBufferPool: creates a new buffer pool with numPages page frames using the page replacement strategy strategy
	b. shutdownBufferPool: destroys a buffer pool.
	c. forceFlushPool: causes all dirty pages (with fix count 0) from the buffer pool to be written to disk
part 2: Buffer Manager Interface Access Pages
	a. markDirty: marks a page as dirty
	b. unpinPage: unpins the page
	c. forcePage: write  the current content of the page back to the page file on disk
	d. pinPage: pinPage pins the page with page number pageNum. The buffer manager is responsible to set the pageNum field of the page handle passed to the method. Similarly, the data field should point to the page frame the page is stored in

part 3: Statistics Interface
	a. getFrameContents: function returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame
	b. getDirtyFlags: function returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame
	c. getNumReadIO: function returns the number of pages that have been read from disk since a buffer pool has been initialized.
	d. getNumWriteIO:  returns the number of pages written to the page file since the buffer pool has been initialized.



