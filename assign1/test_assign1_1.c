#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"
#define TESTMULTIPLE "test1"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);
static void testMultiplePageContent(void);
static void testWrite(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();

  testCreateOpenClose();
  testSinglePageContent();
  //Newly added to test readNextBlock readLastBlock readPreviousBlock readCurrentBlock readBlock on multiple page files
  testMultiplePageContent();
  //Newly added to test write block functions
  testWrite();

  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  testName = "test create open and close methods";

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  TEST_DONE();
}

/* Try to create, open, and close a page file */
/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  

  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
  
    
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading first block\n");



  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));
  //Free memory, if not, will cause memory leak.
  free(ph);  
  
  TEST_DONE();
}



void
testMultiplePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  SM_PageHandle ph1;
  SM_PageHandle ph2;
  int i;

  testName = "test multiple page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);
  ph1 = (SM_PageHandle) malloc(PAGE_SIZE);
  ph2 = (SM_PageHandle) malloc(PAGE_SIZE);

  TEST_CHECK(createPageFile (TESTMULTIPLE));
  TEST_CHECK(openPageFile (TESTMULTIPLE, &fh)); //This file has 3 pages.
  printf("opened file\n");
 
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = 'a';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  for (i=0; i < PAGE_SIZE; i++)
    ph1[i] = 'b';
  TEST_CHECK(writeBlock (1, &fh, ph1));
  printf("writing second block\n");

  for (i=0; i < PAGE_SIZE; i++)
    ph2[i] = 'c';
  TEST_CHECK(writeBlock (2, &fh, ph2));
  printf("writing third block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
	{//printf("ph is %d:",ph[i]);
	    ASSERT_TRUE((ph[i] == 'a'), "character in page read from disk is the one we expected.");
	}
  printf("reading first block\n");

  TEST_CHECK(readNextBlock (&fh, ph1));
  for (i=0; i < PAGE_SIZE; i++)
	{//printf("ph1 is %d:",ph1[i]);
	    ASSERT_TRUE((ph1[i] == 'b'), "character in page read from disk is the one we expected.");
	}
  printf("reading next block\n");

  TEST_CHECK(readPreviousBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
	{//printf("ph is %d:",ph1[i]);
	    ASSERT_TRUE((ph[i] == 'a'), "character in page read from disk is the one we expected.");
	}
  printf("reading Previous block\n");

  TEST_CHECK(readLastBlock (&fh, ph2));
  for (i=0; i < PAGE_SIZE; i++)
	{//printf("ph2 is %d:",ph2[i]);

	    ASSERT_TRUE((ph2[i] == 'c'), "character in page read from disk is the one we expected.");
	}
  printf("reading last block\n");

  TEST_CHECK(readCurrentBlock (&fh, ph2));
  for (i=0; i < PAGE_SIZE; i++)
	{//printf("ph2 is %d:",ph2[i]);

	    ASSERT_TRUE((ph2[i] == 'c'), "character in page read from disk is the one we expected.");
	}
  printf("reading current block\n");

  TEST_CHECK(readBlock (1,&fh, ph1));
  for (i=0; i < PAGE_SIZE; i++)
	{//printf("ph1 is %d:",ph1[i]);

	    ASSERT_TRUE((ph1[i] == 'b'), "character in page read from disk is the one we expected.");
	}
  printf("reading the pageNumth block\n");

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTMULTIPLE));  
  free(ph);
  free(ph1);
  free(ph2);
  TEST_DONE();
}

void testWrite(){
    SM_FileHandle fh;
    SM_PageHandle ph;
    int i;
    
    testName = "test Write";
    
    ph = (SM_PageHandle) malloc(PAGE_SIZE);
    
    // create a new page file
    TEST_CHECK(createPageFile (TESTPF));
    TEST_CHECK(openPageFile (TESTPF, &fh));
    printf("created and opened file\n");
    
    // change ph to be a string and write that one to disk
    for (i=0; i < PAGE_SIZE; i++)
        ph[i] = 'x';
    TEST_CHECK(writeBlock (0, &fh, ph));
    printf("writing first block\n");
    
    // change ph to be a string and write that one to disk
    for (i=0; i < PAGE_SIZE; i++)
        ph[i] = 'y';
    TEST_CHECK(writeBlock (1, &fh, ph));
    printf("writing second block\n");
    
    // read back the page containing the string and check that it is correct
    TEST_CHECK(readFirstBlock (&fh, ph));
    for (i=0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == 'x'), "character in page read from disk is the one we expected.");
    printf("reading first block\n");
    
    // read back the page containing the string and check that it is correct
    TEST_CHECK(readNextBlock (&fh, ph));
    for (i=0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == 'y'), "character in page read from disk is the one we expected.");
    printf("reading second block\n");
    
    for (i=0; i < PAGE_SIZE; i++)
        ph[i] = 'z';
    TEST_CHECK(writeCurrentBlock (&fh, ph));
    printf("writing second block\n");
    
    // read back the page containing the string and check that it is correct
    TEST_CHECK(readCurrentBlock (&fh, ph));
    for (i=0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == 'z'), "character in page read from disk is the one we expected.");
    printf("reading second block\n");
    
    // destroy new page file
    TEST_CHECK(destroyPageFile (TESTPF));  
    free(ph);
    
    TEST_DONE();
}


