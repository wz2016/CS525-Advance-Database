#include "dberror.h"
#include "tables.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include "btree_mgr.h"
#include "buffer_mgr.h"

typedef struct btreeList{
  BTreeHandle * bth;
    int numnodes;
    int numEntries;
} btreeList;

int curpos = 0;

int totaltrees = 0;
btreeList btl[55];

// init and shutdown index manager
RC initIndexManager (void *mgmtData){
    return RC_OK;
}
RC shutdownIndexManager (){
    return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n){
    //printf("create tree!\n");
    BTreeHandle* tree = (BTreeHandle*)malloc(sizeof(BTreeHandle));
    Node* root = (Node*)malloc(sizeof(Node));
    root->nval = 0;
    root->leaf = true;
    tree->mgmtData = root;
    tree->keyType = keyType;
    tree->idxId = idxId;
    btl[totaltrees].bth = tree;
    btl[totaltrees].numnodes = 0;
    btl[totaltrees].numEntries = 0;
    totaltrees++;
    return RC_OK;
}
RC openBtree (BTreeHandle **tree, char *idxId){
    //printf("open tree!\n");
    int i;
    for(i=0;i<totaltrees;i++)
    {
        if(strcmp(idxId,btl[i].bth->idxId)==0)
        {
            *tree = btl[i].bth;
            return RC_OK;
        }
    }
    return RC_IM_NO_MORE_ENTRIES;
}
RC closeBtree (BTreeHandle *tree){
    return RC_OK;
}
RC deleteBtree (char *idxId){
    int i;
    for(i=0;i<totaltrees;i++)
    {
        if(strcmp(idxId,btl[i].bth->idxId)==0)
        {
            free(btl[i].bth);
            btl[i].numnodes = 0;
            btl[i].numEntries = 0;
            totaltrees--;
            return RC_OK;
        }
    }
    return RC_OK;
}

// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result){
    char * trid = tree->idxId;
    int i;
    for(i=0;i<totaltrees;i++)
    {
        if(strcmp(trid,btl[i].bth->idxId)==0)
        {
            *result = btl[i].numnodes;
            return RC_OK;
        }
    }
    return RC_IM_NO_MORE_ENTRIES;
}
RC incNumNodes (BTreeHandle *tree){
    //printf("NODES INCREASED!\n");
    char * trid = tree->idxId;
    int i;
    for(i=0;i<totaltrees;i++)
    {
        if(strcmp(trid,btl[i].bth->idxId)==0)
        {
            btl[i].numnodes = btl[i].numnodes+1;
            return RC_OK;
        }
    }
    return RC_IM_NO_MORE_ENTRIES;
}

RC decNumNodes (BTreeHandle *tree){
    //printf("NODES INCREASED!\n");
    char * trid = tree->idxId;
    int i;
    for(i=0;i<totaltrees;i++)
    {
        if(strcmp(trid,btl[i].bth->idxId)==0)
        {
            btl[i].numnodes = btl[i].numnodes-1;
            return RC_OK;
        }
    }
    return RC_IM_NO_MORE_ENTRIES;
}

RC incNumEntries (BTreeHandle *tree){
    char * trid = tree->idxId;
    int i;
    for(i=0;i<totaltrees;i++)
    {
        if(strcmp(trid,btl[i].bth->idxId)==0)
        {
            btl[i].numEntries = btl[i].numEntries+1;
            return RC_OK;
        }
    }
    return RC_IM_NO_MORE_ENTRIES;
}

RC decNumEntries (BTreeHandle *tree){
    char * trid = tree->idxId;
    int i;
    for(i=0;i<totaltrees;i++)
    {
        if(strcmp(trid,btl[i].bth->idxId)==0)
        {
            btl[i].numEntries = btl[i].numEntries-1;
            return RC_OK;
        }
    }
    return RC_IM_NO_MORE_ENTRIES;
}

RC getNumEntries (BTreeHandle *tree, int *result) {
    char * trid = tree->idxId;
    int i;
    for(i=0;i<totaltrees;i++)
    {
        if(strcmp(trid,btl[i].bth->idxId)==0)
        {
            *result = btl[i].numEntries;
            return RC_OK;
        }
    }
    return RC_IM_NO_MORE_ENTRIES;
}
extern RC getKeyType (BTreeHandle *tree, DataType *result){
    char * trid = tree->idxId;
    int i;
    for(i=0;i<totaltrees;i++)
    {
        if(strcmp(trid,btl[i].bth->idxId)==0)
        {
            *result = btl[i].bth->keyType;
            return RC_OK;
        }
    }
    return RC_IM_NO_MORE_ENTRIES;
}

// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result){
    //printf("finding(%d)\n", key->v.intV);
    int val = key->v.intV;
    Node *nd = (Node *)tree->mgmtData;
    //printf("root: %d,%d\n",nd->vals[0],nd->vals[1]);
    int nnod,nent;
    getNumNodes (tree, &nnod);
    getNumEntries (tree, &nent);
    int i;
    while(1){
        if(nd==NULL || nd->nval==0){
            //printf("error!\n");
            return RC_IM_KEY_NOT_FOUND;
        }
        if(nd->leaf==true){//leaf
            //printf("leaf!!!!%d\n",key->v.intV);
            for(i=0;i<nd->nval;i++){
                if(nd->vals[i]==val && nd->delt[i]==false){
                    result->page = nd->rids[i].page;
                    result->slot = nd->rids[i].slot;
                    return RC_OK;
                }
            }
            return RC_IM_KEY_NOT_FOUND;
        }
        if(val < nd->vals[0]){
            //printf("left(%d<%d)\n",val,nd->vals[0]);
            nd = nd->nds[0];
        }
        else if(val < nd->vals[1]){
            //printf("mid(%d<%d)\n",val,nd->vals[1]);
            nd = nd->nds[1];
        }
        else{
            //printf("right(%d>%d)\n",val,nd->vals[1]);
            nd = nd->nds[2];
        }
    }
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid){
    //printf("inserting(%d)\n", key->v.intV);
    RID res;
    int rt = findKey(tree,key,&res);
    if(rt==RC_OK){
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    Node* root = (Node*)tree->mgmtData;
    if(root->nval==0){
        root->rids[0].page=rid.page;
        root->rids[0].slot=rid.slot;
        root->vals[0]=key->v.intV;
        root->vals[1]=0;
        root->nds[2]=NULL;
        root->nval++;
        incNumNodes(tree);
        incNumEntries(tree);
        //printTree(tree);
        //printf("\n\n\n\n\n\n");
        return RC_OK;
    }
    Node *ret = insertNode(tree,root,key->v.intV,rid);
    if(ret != NULL){
        tree->mgmtData = ret;
        //printf("new root: (%d,%d)\n", ret->vals[0],ret->vals[1]);
        incNumNodes(tree);
    }
    //printTree(tree);
    //printf("\n\n\n\n\n\n");
    return RC_OK;
}

Node *insertNode(BTreeHandle *tree, Node *nd, int val, RID rid){
    Node *ret,*left,*right;
    if(nd->leaf == true){//reach leaf
        if(nd->nval==1){
            nd->nval++;
            if(nd->vals[0] > val){
                nd->vals[1] = nd->vals[0];
                nd->rids[1].page = nd->rids[0].page;
                nd->rids[1].slot = nd->rids[0].slot;
                nd->vals[0] = val;
                nd->rids[0].page = rid.page;
                nd->rids[0].slot = rid.slot;
                incNumEntries(tree);
                return NULL;
            }
            nd->vals[1] = val;
            nd->rids[1].page = rid.page;
            nd->rids[1].slot = rid.slot;
            incNumEntries(tree);
            return NULL;
        }
        //leaf split
        incNumNodes(tree);
        ret = (Node *)malloc(sizeof(Node));
        ret->leaf = false;
        ret->nval = 1;
        right = (Node *)malloc(sizeof(Node));
        right->leaf = true;
        right->nval = 1;
        right->nds[2] = nd->nds[2];
        nd->nds[2] = right;
        nd->leaf = true;
        right->vals[1]=0;
        if(val < nd->vals[0]){
            //printf("split left\n");
            right->vals[0] = nd->vals[1];
            right->rids[0] = nd->rids[1];
            nd->vals[1] = nd->vals[0];
            nd->rids[1].page = nd->rids[0].page;
            nd->rids[1].slot = nd->rids[0].slot;
            nd->vals[0] = val;
            nd->rids[0].page = rid.page;
            nd->rids[0].slot = rid.slot;
        } else if(val < nd->vals[1]){
            //printf("split mid\n");
            right->vals[0] = nd->vals[1];
            right->rids[0].page = nd->rids[1].page;
            right->rids[0].slot = nd->rids[1].slot;
            nd->vals[1] = val;
            nd->rids[1].page = rid.page;
            nd->rids[1].slot = rid.slot;
        }
        else{
            //printf("split right\n");
            right->vals[0] = val;
            right->rids[0].page = rid.page;
            right->rids[0].slot = rid.slot;
        }
        ret->nds[0] = nd;
        ret->nds[1] = right;
        ret->vals[0] = right->vals[0];
        incNumEntries(tree);
        //printf("ret tree is: (%d.%d)-(%d.%d)-(%d.%d)(%d.%d.%d)\n",ret->nds[0]->vals[0],ret->nds[0]->vals[1],ret->vals[0],ret->vals[1],ret->nds[0]->nds[2]->vals[0],ret->nds[0]->nds[2]->vals[1],ret->nds[0]->leaf?0:1,ret->leaf?0:1,ret->nds[0]->nds[2]->leaf?0:1);
        //printf("ret tree is2: (%d.%d)-(%d.%d)-(%d.%d)(%d.%d.%d)\n",nd->vals[0],nd->vals[1],ret->vals[0],ret->vals[1],right->vals[0],right->vals[1],nd->leaf?0:1,ret->leaf?0:1,right->leaf?0:1);
        return ret;
    }
    //non leaf
    if(nd->nval==1){
        Node *rt = NULL;
        if(nd->vals[0]>val){
            rt = insertNode(tree,nd->nds[0],val,rid);
        } else {
            rt = insertNode(tree,nd->nds[1],val,rid);
        }
        if(rt != NULL){//leaf split add
            nd->nval++;
            //printf("split add: %d\n", rt->vals[0]);
            if(rt->vals[0]<nd->vals[0]){
                nd->vals[1] = nd->vals[0];
                nd->vals[0] = rt->vals[0];
                nd->nds[2] = nd->nds[1];
                nd->nds[0] = rt->nds[0];
                nd->nds[1] = rt->nds[1];
                return NULL;
            }
            nd->vals[1] = rt->vals[0];
            nd->nds[1] = rt->nds[0];
            nd->nds[2] = rt->nds[1];
            return NULL;
        }
        return NULL;
    }
    Node *rt = NULL;
    if(nd->vals[0]>val){
        rt = insertNode(tree,nd->nds[0],val,rid);
    } else if(nd->vals[1]>val) {
        rt = insertNode(tree,nd->nds[1],val,rid);
    } else {
        rt = insertNode(tree,nd->nds[2],val,rid);
    }
    if(rt != NULL){//non leaf split
        //printf("non-leaf split!\n");
        incNumNodes(tree);
        ret = (Node *)malloc(sizeof(Node));
        ret->leaf = false;
        ret->nval = 1;
        right = (Node *)malloc(sizeof(Node));
        right->leaf = false;
        right->nval = 1;
        left = (Node *)malloc(sizeof(Node));
        left->leaf = false;
        left->nval = 1;
        if(rt->vals[0]<nd->vals[0]){
            //printf("left non leaf!!\n");
            ret->vals[0] = nd->vals[0];
            ret->nds[0] = left;
            ret->nds[1] = right;
            right->vals[0] = nd->vals[1];
            right->nds[0] = nd->nds[1];
            right->nds[1] = nd->nds[2];
            left->vals[0] = rt->vals[0];
            left->nds[0] = rt->nds[0];
            left->nds[1] = rt->nds[1];
        } else if(rt->vals[0]<nd->vals[1]){
            ret->vals[0] = rt->vals[0];
            ret->nds[0] = left;
            ret->nds[1] = right;
            right->vals[0] = nd->vals[1];
            right->nds[0] = rt->nds[1];
            right->nds[1] = nd->nds[2];
            left->vals[0] = nd->vals[0];
            left->nds[0] = nd->nds[0];
            left->nds[1] = rt->nds[0];
        } else {
            ret->vals[0] = nd->vals[1];
            ret->nds[0] = left;
            ret->nds[1] = right;
            right->vals[0] = rt->vals[0];
            right->nds[0] = rt->nds[0];
            right->nds[1] = rt->nds[1];
            left->vals[0] = nd->vals[0];
            left->nds[0] = nd->nds[0];
            left->nds[1] = nd->nds[1];
        }
        free(rt);
        return ret;
    }
    return NULL;
}


RC deleteKey (BTreeHandle *tree, Value *key){
    RID res;
    int rt = findKey(tree,key,&res);
    if(rt!=RC_OK){
        return rt;
    }
    printf("delete(%d)\n", key->v.intV);
    int val = key->v.intV;
    Node *nd = (Node *)tree->mgmtData;
    int nnod,nent;
    getNumNodes (tree, &nnod);
    getNumEntries (tree, &nent);
    int i;
    while(1){
        if(nd==NULL || nd->nval==0){
            printf("error!\n");
            return RC_IM_KEY_NOT_FOUND;
        }
        if(nd->leaf==true){//leaf
            //printf("leaf!!!!%d\n",key->v.intV);
            for(i=0;i<nd->nval;i++){
                if(nd->vals[i]==val && nd->delt[i]==false){
                    nd->delt[i]=true;
                    return RC_OK;
                }
            }
            return RC_IM_KEY_NOT_FOUND;
        }
        if(val < nd->vals[0]){
            //printf("left(%d<%d)\n",val,nd->vals[0]);
            nd = nd->nds[0];
        }
        else if(val < nd->vals[1]){
            //printf("mid(%d<%d)\n",val,nd->vals[1]);
            nd = nd->nds[1];
        }
        else{
            //printf("right(%d>%d)\n",val,nd->vals[1]);
            nd = nd->nds[2];
        }
    }
}


RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){
    curpos = 1;
    Node * nd = (Node*)tree->mgmtData;
    //printTree(tree);
    while(nd->leaf==false){
        nd = nd->nds[0];
    }
    BT_ScanHandle *sh = (BT_ScanHandle *)malloc(sizeof(BT_ScanHandle));
    sh->tree = tree;
    sh->mgmtData = nd;
    *handle = sh;
    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result){
    //printf("next!\n");
    Node * nd = (Node *)handle->mgmtData;
    if(curpos==-1){
        return RC_IM_NO_MORE_ENTRIES;
    }
    if(curpos==1){
        //printf("fond: %d\n", nd->vals[0]);
        result->page = nd->rids[0].page;
        result->slot = nd->rids[0].slot;
        curpos = 2;
        return RC_OK;
    } else if(curpos==2 && nd->vals[1]!=0){
        //printf("fond 2: %d\n", nd->vals[1]);
        result->page = nd->rids[1].page;
        result->slot = nd->rids[1].slot;
        nd = nd->nds[2];
        handle->mgmtData = nd;
        if(nd==NULL){
            curpos = -1;
            return RC_OK;
        }
        curpos = 1;
        return RC_OK;
    }
    nd = nd->nds[2];
    handle->mgmtData = nd;
    if(nd==NULL){
        return RC_IM_NO_MORE_ENTRIES;
    }
    result->page = nd->rids[0].page;
    result->slot = nd->rids[0].slot;
    curpos = 2;
    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle){
    return RC_OK;
}

// debug and test functions
char *printTree (BTreeHandle *tree){
    Node* nd = (Node *)tree->mgmtData;
    printNode(nd,0);
    return "";
}

void printNode(Node *nd, int ct){
    printf(" (%d){%d,%d}(%d)(%d)\n", ct, nd->vals[0],nd->vals[1],nd->leaf?0:1,nd->nds[2]==NULL?0:1);
    if(nd->leaf == false){
        printNode(nd->nds[0], ct+1);
        printNode(nd->nds[1], ct+1);
        if(nd->nval==2){
            printNode(nd->nds[2],ct+1);
        }
    }
}
