#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"

typedef struct tableList
{
    char * name;
    Schema *schema;
    int schemaSize;
    int totalTuples;
    BM_BufferPool *bm;
}tableList;
typedef struct scanHandle
{
    int curPos;
    char * data;
    Expr * cond;
}scanHandle;

typedef struct BufferFrame
{
    BM_PageHandle ph;
    int fixcount;
    bool dirty;
    int recordPos;
    struct BufferFrame *next;
}BufferFrame;


int totalTables = 0;
tableList tb[50];

//table and manager
RC initRecordManager (void *mgmtData){
    return RC_OK;
}

RC shutdownRecordManager (){
    return RC_OK;
}

RC createTable (char *name, Schema *schema)
{

    FILE * pFile;  
    pFile=fopen(name, "wb");


    BM_BufferPool *bm;
    BufferFrame* bf;
	bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
	int rc = initBufferPool(bm, name, 60, RS_FIFO, NULL);
    bf = bm->mgmtData;
    bf->ph.pageNum = 1;
    
    tb[totalTables].name = name;
    tb[totalTables].schema = schema;
    tb[totalTables].totalTuples = 0;
    tb[totalTables].schemaSize = ftell(pFile);
  	tb[totalTables].bm = bm;
    
    totalTables++;
    fclose(pFile);
    
    return RC_OK;
}

RC openTable (RM_TableData *rel, char *name)
{
    int i;
    
    for(i=0;i<totalTables;i++)
        if(strcmp(tb[i].name, name) == 0)
            break;

    rel->name = name;
    rel->schema = tb[i].schema;
    
    rel->mgmtData = tb[i].bm;
    
    return RC_OK;
}


RC closeTable (RM_TableData *rel)
{
    return RC_OK;
}

RC deleteTable (char *name)
{
    int i;
    for(i=0;i<totalTables;i++)
        if(strcmp(tb[i].name,name) == 0)
            break;
 
    for(;i+1<totalTables;i++)
    {
        strcpy(tb[i].name, tb[i+1].name);
        tb[i].schema = tb[i+1].schema;
        tb[i].totalTuples = tb[i+1].totalTuples;
        tb[i].schemaSize = tb[i+1].schemaSize;
        tb[i].bm = tb[i+1].bm;
    }
    
    totalTables -= 1;
    
    return RC_OK;
}

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    scanHandle * s;
    char * data;
    
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BufferFrame *bf = (BufferFrame *)bm->mgmtData ;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    page->data = bf->ph.data;
    page->pageNum = bf->ph.pageNum;
    
    s = (scanHandle *)malloc(sizeof(scanHandle));
    s->data = page->data;
    s->curPos = 0;
    s->cond = cond;
    
    scan->rel = rel;
    scan->mgmtData = s;
    
    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record)
{
    Record *r = (Record *)malloc(sizeof(Record));
    r->data = (char *)malloc(sizeof(char));
    RM_TableData *t = (RM_TableData *)scan->rel;
    Schema *sc = (Schema *) t->schema;
    scanHandle * sh = (scanHandle *)scan->mgmtData;
    Expr *cond = (Expr *)sh->cond;
    int recordSize = getRecordSize(sc);
    int tuples = getNumTuples(t);
    char * data = (char *)malloc(sizeof(char));
    data = (char *)sh->data;
    
    if(cond == NULL)
    {
        if(sh->curPos >= ((recordSize+sizeof(int) + sizeof(char)) * tuples))
            return RC_RM_NO_MORE_TUPLES;
        
        record->data = (char *)(data + sh->curPos + sizeof(int));
        sh->curPos = sh->curPos + recordSize + sizeof(int) + sizeof(char);
        scan->mgmtData = sh;
    }
    else
    {
        while(true)
        {
            if(sh->curPos >= ((recordSize+sizeof(int) + sizeof(char)) * tuples))
                return RC_RM_NO_MORE_TUPLES;
			RID id;
			Value *result;
            id.slot = sh->curPos;
            getRecord(t, id, r);
            evalExpr(r, sc, cond, &result);
            if(result->v.boolV)
            {
                *(record) = *(r);
                sh->curPos = sh->curPos + recordSize + sizeof(int) + sizeof(char);
                scan->mgmtData = sh;
                break;
            }
            
            sh->curPos = sh->curPos + recordSize + sizeof(int) + sizeof(char);
        }
    }
    return RC_OK;
}

RC closeScan (RM_ScanHandle *scan)
{   
    return RC_OK;
}


int getRecordSize (Schema *schema)
{
    DataType * dt;
    dt = (DataType *)malloc(sizeof(DataType));
    dt = schema->dataTypes;
    int num = schema->numAttr;
    
	int i;
	int size =0;
    for(i=0;i<num;i++)
    {
        if(dt[i] == DT_INT)
            size += sizeof(int);
        else if(dt[i] == DT_STRING)
            size += schema->typeLength[i] * sizeof(char);
        else if(dt[i] == DT_FLOAT)
            size += sizeof(float);
        else if(dt[i] == DT_BOOL)
            size += sizeof(bool);
        else
        {
            return -1;
        }
    }
    
    return size;
}

int getNumTuples (RM_TableData *rel)
{
    char * name = rel->name;
    
	int i;
    for(i=0;i<totalTables;i++)
        if(strcmp(name,tb[i].name)==0)
            return tb[i].totalTuples;
    
    return -1;
}

RC insertRecord (RM_TableData *rel, Record *record)
{
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BufferFrame *bf = (BufferFrame *)bm->mgmtData ;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    
	//get the table number from the tablelist by check the name of table
	int i;
	char * name = rel->name;
    for(i=0;i<totalTables;i++)
        if(strcmp(name,tb[i].name)==0)
            break;

	//each record is followed by a tombstone mark and a seperater
    int recordSize = getRecordSize(rel->schema) + sizeof(int)+sizeof(char);
    int totalTuples = getNumTuples(rel);
    int recordPerPage = PAGE_SIZE/recordSize;
    int pageNum = totalTuples/recordPerPage + 1;
    
    int slot = totalTuples * (recordSize);
    
    page->pageNum = pageNum;
    (bf->ph).pageNum = pageNum;
    page->data = (bf->ph).data;
    
    record->id.page = page->pageNum;
    record->id.slot = slot;
    
    char * temp = (char *)malloc(recordSize);
	//set tombstone of a record to 0
    *((int *) temp) = 0; 
    memcpy(temp + sizeof(int), record->data, recordSize-sizeof(int)-sizeof(char));
	//add a separater after each record
    *((char *) temp+recordSize-sizeof(char)) = '\n';
    
    memcpy((page->data+slot), temp, recordSize);
    
    markDirty(bm, page);

    free(temp);
    
    tb[i].totalTuples += 1;
    
    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id)
{
    int  slot = id.slot;
    char *name = rel->name;
    
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BufferFrame *bf = (BufferFrame *)bm->mgmtData ;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

    page->pageNum = bf -> ph.pageNum;
    page->data = bf -> ph.data;
    
	//set tombstone = 1
    *((int*)(page->data)+slot) = 1;
    
    name = rel->name;
	int i;
    for(i=0;i<totalTables;i++)
        if(strcmp(name,tb[i].name)==0)
            break;
    
    tb[i].totalTuples -= 1;
    
    return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record)
{   
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BufferFrame *bf = (BufferFrame *)bm->mgmtData ;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	int slot = record->id.slot;
    int recordSize = getRecordSize(rel->schema);
    page->data = bf -> ph.data;
    
    memcpy((page->data)+slot+sizeof(int), record->data, recordSize);
    return RC_OK;
    
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BufferFrame *bf = (BufferFrame *)bm->mgmtData ;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    
	int slot = id.slot;
    int recordSize = getRecordSize(rel->schema);

    page->pageNum = (bf -> ph).pageNum;
    page->data = (bf -> ph).data;
    
    memcpy(record->data, ((page->data)+slot+sizeof(int)), recordSize);
    record->id.slot = id.slot;
    record->id.page = id.page;
    
    return RC_OK;

}

//deal with schemas

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    
    schema->numAttr = numAttr;
    schema->attrNames = (char **)malloc(sizeof(char*)*numAttr);
 
    schema->dataTypes = (DataType *)malloc(sizeof(DataType)*numAttr);
    
    schema->typeLength = (int *)malloc(sizeof(int)*numAttr);
    
    schema->keySize = keySize;
    schema->keyAttrs = (int *)malloc(sizeof(int)*keySize);
    
    int i;
    for (i=0; i<numAttr; i++) {
        schema->attrNames[i]=attrNames[i];
        schema->dataTypes[i]=dataTypes[i];
        schema->typeLength[i]=typeLength[i];
    }
    for (i=0; i<keySize; i++) {
        schema->keyAttrs[i] = keys[i];
    }
    
    return schema;
}

//free created schema
RC freeSchema (Schema *schema){
    if(!schema){
        return RC_OK;
    }
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);
    return RC_OK;
}
// dealing with records and attribute values

RC createRecord (Record **record, Schema *schema){
    Record *r;
//    RID *id;
    RID id;
    char *data;
    
    r = (Record *)malloc(sizeof(Record));
    data = (char *)malloc(sizeof(char)*getRecordSize(schema));
//    id = (RID *)malloc(sizeof(RID));
    
    id.slot = 0;
    id.page = 0;
//    r->id = *id;
    r->id = id;
    r->data = data;
    *(record) = r;
    
    return RC_OK;
}

RC freeRecord (Record *record){
    free(record->data);
//    free(record->id);
    free(record);
    
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    Value *v = (Value *)malloc(sizeof(Value));
    int i;
    int offset = 0;
    for(i=0;i<attrNum;i++){
        if (schema->dataTypes[i]==DT_STRING) {
            offset = offset + (schema->typeLength[i])*sizeof(char);
        }
        else{
            offset = offset + sizeof(schema->dataTypes[i]);
        }
    }
    
    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            memcpy(&(v->v.intV),&(record->data[offset]), sizeof(int));
            v->dt=DT_INT;
            break;
            
        case DT_STRING:
            v->v.stringV = malloc((schema->typeLength[i])*sizeof(char));
            memcpy(v->v.stringV, &(record->data[offset]), (schema->typeLength[i])*sizeof(char));
            v->dt=DT_STRING;
            break;
            
        case DT_FLOAT:
            memcpy(&(v->v.floatV), &(record->data[offset]), sizeof(float));
            v->dt=DT_FLOAT;
            break;
            
        case DT_BOOL:
            memcpy(&(v->v.boolV), &(record->data[offset]), sizeof(bool));
            v->dt = DT_BOOL;
            break;
            
        default:
            break;
            
    }
    (*value) = v;
    return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    int offset = 0;
    int i ;
    for(i=0;i<attrNum;i++){
        if(schema->dataTypes[i]==DT_STRING){
            offset = offset +(schema->typeLength[i])*sizeof(char);
        }
        else{
        offset = offset + sizeof(schema->dataTypes[i]);
        }
    }
    switch (value->dt) {
        case DT_INT:
            memcpy(&(record->data[offset]), &value->v.intV, sizeof(int));
            break;
            
        case DT_STRING:
            memcpy(&(record->data[offset]), value->v.stringV, strlen(value->v.stringV));
            break;
        case DT_FLOAT:
            memcpy(&(record->data[offset]),&value->v.floatV,sizeof(float));
            break;
        case DT_BOOL:
            memcpy(&(record->data[offset]), &value->v.boolV, sizeof(bool));
            break;
    }
    return RC_OK;
}
                
