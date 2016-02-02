#include <stdlib.h>
#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"

void printtable(char* tableName);

int
main (void) 
{
    initRecordManager(NULL);
    int in;
    int cl = 0;
    while(cl == 0){
        printf("What to do:\n1.Add new table\n2.insert\n3.update\n4.delete\n5.scan\n6.print table\n7.close\n");
        in=0;
        scanf("%d",&in);
        RM_TableData *table;
        Record *result,*r;
        Value *value,*v;
        int i,j;
        switch(in)
        {
            case 0:
                printf("invalid input\n");
                break;
            case 1:
                printf("\nAdding new table:\n");
                printf("Table name: ");
                char tbn[50];
                scanf("%s",tbn);
                char* tbnp = (char *) malloc(sizeof(char)*sizeof(tbn));
                memcpy(tbnp,tbn,sizeof(tbn));
                printf("number of attributes: ");
                int nat = 0;
                scanf("%d",&nat);
                printf("number of keys: ");
                int nk = 0;
                scanf("%d",&nk);
                Schema *sch;
                char **names = (char **) malloc(sizeof(char*) * nat);
                DataType* dt = (DataType *) malloc(sizeof(DataType) * nat);
                int* sizes = (int *) malloc(sizeof(int) * nat);
                int* keys = (int *) malloc(sizeof(int) * nk);
                int kptr = 0;
                for(i=0;i<nat;i++){
                    printf("Attribute %d name:  ", i+1);
                    char buf[100];
                    scanf("%s", buf);
                    char* tmp = (char *) malloc(sizeof(char)*sizeof(buf));
                    memcpy(tmp,buf,sizeof(buf));
                    names[i] = tmp;
                    printf("Attribute %d datatype: [I/S/F/B]: ", i+1);
                    char dtp;
                    scanf(" %c", &dtp);
                    //printf("\ndtp = %c", dtp);
                    switch(dtp)
                    {
                        case 'I':
                            dt[i] = DT_INT;
                            sizes[i] = 0;
                            break;
                        case 'S':
                            dt[i] = DT_STRING;
                            printf("Attribute %d length:  ", i+1);
                            scanf("%d",&sizes[i]);
                            printf("length is: %d\n", sizes[i]);
                            break;
                        case 'F':
                            dt[i] = DT_FLOAT;
                            sizes[i] = 0;
                            break;
                        case 'B':
                            dt[i] = DT_BOOL;
                            sizes[i] = 0;
                            break;
                            
                    }
                    printf("Is Attribute %d a key?: [Y/N] ", i+1);
                    char isk;
                    scanf(" %c", &isk);
                    if(isk == 'Y'){
                        keys[kptr] = i;
                        kptr++;
                    }
                    //printf("schema add: %s,%d,%c\n",names[i],sizes[i],isk);
                }
                sch = createSchema(nat,names,dt,sizes,nk,keys);
                createTable(tbnp,sch);
                break;
            case 2:
                printf("Insert record\n");
                printf("table name: ");
                char buf2[100];
                scanf("%s", buf2);
                char* tmp2 = (char *) malloc(sizeof(char)*sizeof(buf2));
                memcpy(tmp2,buf2,sizeof(buf2));
                table = (RM_TableData *) malloc(sizeof(RM_TableData));
                openTable(table, tmp2);
                createRecord(&result, table->schema);
               for(i=0;i<table->schema->numAttr;i++){
                    if(table->schema->dataTypes[i] == DT_INT)
                    {
                        printf("intput int attr[%d/%d] value -- %s: ", i+1,table->schema->numAttr,table->schema->attrNames[i]);
                        int intt = 0;
                        scanf("%d",&intt);
                        //printf("\n%d is %d\n", i, intt);
                        MAKE_VALUE(value,DT_INT,intt);
                        setAttr(result, table->schema, i, value);
                        freeVal(value);
                    } else if (table->schema->dataTypes[i] == DT_STRING)
                    {
                        printf("intput string attr[%d/%d] value -- %s: ", i+1,table->schema->numAttr,table->schema->attrNames[i]);
                        char instrbuf[100];
                        scanf("%s",instrbuf);
                        char* instr = (char *) malloc(sizeof(char)*sizeof(instrbuf));
                        memcpy(instr,instrbuf,sizeof(instrbuf));
                        MAKE_STRING_VALUE(value,instr);
                        setAttr(result, table->schema, i, value);
                        freeVal(value);
                    } else if (table->schema->dataTypes[i] == DT_FLOAT)
                    {
                        printf("intput float attr[%d/%d] value -- %s: ", i+1,table->schema->numAttr,table->schema->attrNames[i]);
                        float infl;
                        scanf("%fl",&infl);
                        MAKE_VALUE(value,DT_FLOAT,infl);
                        setAttr(result, table->schema, i, value);
                        freeVal(value);
                    } else if (table->schema->dataTypes[i] == DT_BOOL)
                    {
                        printf("intput boolean attr[%d/%d] value -- %s [T/F]: ", i+1,table->schema->numAttr,table->schema->attrNames[i]);
                        char inbl;
                        scanf(" %c",&inbl);
                        if(inbl == 'T'){
                            MAKE_VALUE(value,DT_BOOL,true);
                        }
                        else if (inbl == 'F'){
                            MAKE_VALUE(value,DT_BOOL,false);
                        }
                        setAttr(result, table->schema, i, value);
                        freeVal(value);
                    }
                    if(i==table->schema->numAttr-1){
                        printf("\n");
                    }
                }
                insertRecord(table,result);
                
                break;
            case 3:
                printf("update record\n");
                printf("table name: ");
                char buf3[100];
                scanf("%s", buf3);
                char* tmp3 = (char *) malloc(sizeof(char)*sizeof(buf3));
                memcpy(tmp3,buf3,sizeof(buf3));
                table = (RM_TableData *) malloc(sizeof(RM_TableData));
                openTable(table, tmp3);
                createRecord(&result, table->schema);
                
                r = (Record *) malloc(sizeof(Record));
                RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
                startScan(table, sc, NULL);
                
                printf("Select record to update: \n");
                j=0;
                int rids[100][2];
                while(next(sc, r) == RC_OK){
                    printf("(%d)   ",j+1);
                    rids[j][0] = r->id.page;
                    rids[j][1] = r->id.slot;
                    for(i=0;i<table->schema->numAttr;i++){
                        if(table->schema->dataTypes[i] == DT_INT)
                        {
                            getAttr(r,table->schema,i,&v);
                            printf("%-15d",v->v.intV);
                        } else if(table->schema->dataTypes[i] == DT_STRING)
                        {
                            getAttr(r,table->schema,i,&v);
                            printf("%-15s",v->v.stringV);
                        } else if(table->schema->dataTypes[i] == DT_FLOAT)
                        {
                            getAttr(r,table->schema,i,&v);
                            printf("%-15fl",v->v.floatV);
                        } else if(table->schema->dataTypes[i] == DT_BOOL)
                        {
                            getAttr(r,table->schema,i,&v);
                            printf("%-15s",v->v.boolV ? "true" : "false");
                        }
                    }
                    printf("\n");
                    j++;
                }
                
                int sele;
                scanf("%d", &sele);
                sele--;
                if(sele >= 0 && sele < j+1){
                    createRecord(&result, table->schema);
                    for(i=0;i<table->schema->numAttr;i++){
                        if(table->schema->dataTypes[i] == DT_INT)
                        {
                            printf("intput int attr[%d/%d] value -- %s: ", i+1,table->schema->numAttr,table->schema->attrNames[i]);
                            int intt = 0;
                            scanf("%d",&intt);
                            //printf("\n%d is %d\n", i, intt);
                            MAKE_VALUE(value,DT_INT,intt);
                            setAttr(result, table->schema, i, value);
                            freeVal(value);
                        } else if (table->schema->dataTypes[i] == DT_STRING)
                        {
                            printf("intput string attr[%d/%d] value -- %s: ", i+1,table->schema->numAttr,table->schema->attrNames[i]);
                            char instrbuf[100];
                            scanf("%s",instrbuf);
                            char* instr = (char *) malloc(sizeof(char)*sizeof(instrbuf));
                            memcpy(instr,instrbuf,sizeof(instrbuf));
                            MAKE_STRING_VALUE(value,instr);
                            setAttr(result, table->schema, i, value);
                            freeVal(value);
                        } else if (table->schema->dataTypes[i] == DT_FLOAT)
                        {
                            printf("intput float attr[%d/%d] value -- %s: ", i+1,table->schema->numAttr,table->schema->attrNames[i]);
                            float infl;
                            scanf("%fl",&infl);
                            MAKE_VALUE(value,DT_FLOAT,infl);
                            setAttr(result, table->schema, i, value);
                            freeVal(value);
                        } else if (table->schema->dataTypes[i] == DT_BOOL)
                        {
                            printf("intput boolean attr[%d/%d] value -- %s [T/F]: ", i+1,table->schema->numAttr,table->schema->attrNames[i]);
                            char inbl;
                            scanf(" %c",&inbl);
                            if(inbl == 'T'){
                                MAKE_VALUE(value,DT_BOOL,true);
                            }
                            else if (inbl == 'F'){
                                MAKE_VALUE(value,DT_BOOL,false);
                            }
                            setAttr(result, table->schema, i, value);
                            freeVal(value);
                        }
                        if(i==table->schema->numAttr-1){
                            printf("\n");
                        }
                    }
                    result->id.page = rids[sele][0];
                    result->id.slot = rids[sele][1];
                    updateRecord(table,result);
                }
                break;
            case 4:
                printf("delete record\n");
                printf("table name: ");
                char buf4[100];
                scanf("%s", buf4);
                char* tmp4 = (char *) malloc(sizeof(char)*sizeof(buf3));
                memcpy(tmp4,buf4,sizeof(buf4));
                table = (RM_TableData *) malloc(sizeof(RM_TableData));
                openTable(table, tmp4);
                createRecord(&result, table->schema);
                
                r = (Record *) malloc(sizeof(Record));
                RM_ScanHandle *sc2 = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
                startScan(table, sc2, NULL);
                
                printf("Select record to delete: \n");
                j=0;
                int rids2[100][2];
                while(next(sc2, r) == RC_OK){
                    printf("(%d)   ",j+1);
                    rids2[j][0] = r->id.page;
                    rids2[j][1] = r->id.slot;
                    for(i=0;i<table->schema->numAttr;i++){
                        if(table->schema->dataTypes[i] == DT_INT)
                        {
                            getAttr(r,table->schema,i,&v);
                            printf("%-15d",v->v.intV);
                        } else if(table->schema->dataTypes[i] == DT_STRING)
                        {
                            getAttr(r,table->schema,i,&v);
                            printf("%-15s",v->v.stringV);
                        } else if(table->schema->dataTypes[i] == DT_FLOAT)
                        {
                            getAttr(r,table->schema,i,&v);
                            printf("%-15fl",v->v.floatV);
                        } else if(table->schema->dataTypes[i] == DT_BOOL)
                        {
                            getAttr(r,table->schema,i,&v);
                            printf("%-15s",v->v.boolV ? "true" : "false");
                        }
                    }
                    printf("\n");
                    j++;
                }
                
                int sele2;
                scanf("%d", &sele2);
                sele2--;
                if(sele2 >= 0 && sele2 < j+1){
                    RID rid;
                    rid.page = rids[sele2][0];
                    rid.slot = rids[sele2][1];
                    deleteRecord(table,rid);
                }
                break;
            case 5:
                printf("scan\n");
                break;
            case 6:
                printf("table name: ");
                char buf[100];
                scanf("%s", buf);
                char* tmp = (char *) malloc(sizeof(char)*sizeof(buf));
                memcpy(tmp,buf,sizeof(buf));
                printtable(buf);
                break;
            case 7:
                cl = 1;
                break;
            default:
                printf("invalid input\n");
                break;
        }
    }
}

void printtable(char* tableName){
    RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
    Record *r = (Record *) malloc(sizeof(Record));
    openTable(table, tableName);
    int i;
    printf("#######################################################\n");
    for(i=0;i<table->schema->numAttr;i++){
        printf("%-15s", table->schema->attrNames[i]);
    }
    printf("\n");
    
    RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
    startScan(table, sc, NULL);
    Value* v;
    while(next(sc, r) == RC_OK){
        for(i=0;i<table->schema->numAttr;i++){
            if(table->schema->dataTypes[i] == DT_INT)
            {
                getAttr(r,table->schema,i,&v);
                printf("%-15d",v->v.intV);
            } else if(table->schema->dataTypes[i] == DT_STRING)
            {
                getAttr(r,table->schema,i,&v);
                printf("%-15s",v->v.stringV);
            } else if(table->schema->dataTypes[i] == DT_FLOAT)
            {
                getAttr(r,table->schema,i,&v);
                printf("%-15fl",v->v.floatV);
            } else if(table->schema->dataTypes[i] == DT_BOOL)
            {
                getAttr(r,table->schema,i,&v);
                printf("%-15s",v->v.boolV ? "true" : "false");
            }
        }
        printf("\n");
    }
    
    printf("#######################################################\n");
}