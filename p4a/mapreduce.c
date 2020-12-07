#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mapreduce.h"


typedef struct PairNode {
    char* key;
    char* value;
    struct PairNode* nextPair;  // pointer to next PairNode
} PairNode;

// typedef struct 

typedef struct PartitionNode {
    PairNode* pairHead;
    PairNode* backUpHead; // parent of head
    // Linked list of keys corresponding to the same hash value
    pthread_mutex_t partitionLock;  // to lock each partition
    char* lastKey;
} PartitionNode;

// per unique key
typedef struct KeyNode {
    char* key;
    struct PairNode* pairHead;
    struct KeyNode* nextKeyNode;
} KeyNode;

// per mapper
typedef struct KeySet {
    pthread_t tid;
    KeyNode* keyHead; 
    int keyNum;
} KeySet;

KeySet** tTable_;
PartitionNode** hTable_;

int _totalFiles;  // total number of files mappers need to process = argc - 1
int _curFile;     // index of the processing file that we are providing mutex on
int _numMappers; // number of mappers to create
int _numPartitions;  // number of partitions to create
int _curPartition;   // index of the reducing partition that we are providing
                     // mutex on
char** _argv;        // a copy of the argv for mapper to call

Mapper _mapper;
Combiner _combiner;
Reducer _reducer;
Partitioner _partitioner;

// locks used to provide mutex for mapper and reducers
pthread_mutex_t _mapLock;
pthread_mutex_t _reduceLock;

/* Hash Function Section */
unsigned long MR_DefaultHashPartition(char* key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0') hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char* key, int num_partitions) {
    unsigned long hash = (unsigned long)atoll(key);
    int mask = 0x0FFFFFFFF;
    hash = hash & mask;
    int counter = 0;
    while ((num_partitions = num_partitions >> 1)) {
        counter++;
    }
    return hash >> (32 - counter);
}

/* Constructors */
// PairNode initializer with certain key, value
PairNode* initPairNode(char* key, char* value) {
    PairNode* newPairNode = (PairNode*)malloc(sizeof(PairNode));
    newPairNode->key = malloc(100);
    newPairNode->key = key;
    newPairNode->value = value;
    newPairNode->nextPair = NULL;
    return newPairNode;
}

// Partition initializer
PartitionNode* initPartition() {
    PartitionNode* newPartition = (PartitionNode*)malloc(sizeof(PartitionNode));
    // Changed malloc pairHead to null
    newPartition->pairHead = NULL;
    newPartition->backUpHead = NULL;
    int rc = pthread_mutex_init(&newPartition->partitionLock, NULL);
    assert(!rc);
    newPartition->lastKey = NULL;
    return newPartition;
}

// KeyNode initializer with given key and pairHead
KeyNode* initKeyNode(char* key, PairNode* pairHead) {
    KeyNode* newKeyNode = (KeyNode*)malloc(sizeof(KeyNode));
    newKeyNode->key = key;
    newKeyNode->pairHead = pairHead;
    newKeyNode->nextKeyNode = NULL;
    return newKeyNode;
}

// KeySet initializer with given thread id
KeySet* initKeySet(pthread_t tid) {
    KeySet* newKeySet = (KeySet*)malloc(sizeof(KeySet));
    newKeySet->tid = tid;
    newKeySet->keyHead = NULL;
    newKeySet->keyNum = 0;
    return newKeySet;
}


// Hashtable initializer
void initTable() {
    hTable_ = (PartitionNode**)malloc(sizeof(PartitionNode*) * _numPartitions);
    for (int i = 0; i < _numPartitions; i++) {
        hTable_[i] = initPartition();
    }
}

void initThreadTable() {
    tTable_ = (KeySet**) malloc(sizeof(KeySet*) * _numMappers);
    /*
    for (int i = 0; i < _numMappers; i++) {
        tTable_[i] = initKeySet(-1); 
    }
    */
}


KeySet* findKeySet(pthread_t tid) {
    for (int i = 0; i < _numMappers; i++) {
        if (tTable_[i] == NULL) {
            return NULL;
        } else {
            if (tid == tTable_[i]->tid) 
                return (KeySet*) tTable_[i];
        }
    }
    return NULL; // on failiure
}


int insertKeySet(KeySet* keyset) {
    for (int i = 0; i < _numMappers; i++) {
        if (tTable_[i] == NULL) {
            tTable_[i] = keyset;
            return 0;
        }
    }
    printf("ERROR: unable to insert keyset"); //FIXME
    return -1; // on failure
}


/* Memory clean up section */

// Frees a single partition
void freePartition(PartitionNode* p) {
    PairNode* temp;
    // The original list has been transported to the new list
    // Thus, just free the new list
    while (p->backUpHead != NULL) {
        temp = p->backUpHead->nextPair;
        // Notice we have to free the key and value for each pair
        free(p->backUpHead->key);
        free(p->backUpHead->value);
        free(p->backUpHead);
        p->backUpHead = temp;
    }
    pthread_mutex_destroy(&(p->partitionLock));
    free(p);
}

// Free stage starter
void freeTable() {
    for (int i = 0; i < _numPartitions; i++) {
        freePartition(hTable_[i]);
    }
    free(hTable_);
}

void freeThreadTable() {
    for (int i = 0; i < _numPartitions; i++) {
        // free(tTable_[i]); //FIXME: assumed that each keyset is empty at this point
    }
    free(tTable_);
}

/* Mapping Phase */

void MR_EmitToCombiner(char *key, char *value){
    // printf("IN MT EMIT TO COMBINE\n"); //DELETE THIS
    // if no combiner is provided, redirect input to reducer
    if (_combiner == NULL) {
        MR_EmitToReducer(key, value);
        return;
    }

    // init new PairNode to insert
    char* storeKey = malloc(strlen(key) + 1);
    char* headKey = malloc(strlen(key) + 1);
    char* storeValue = malloc(strlen(value) + 1);
    strcpy(storeKey, key);
    strcpy(storeValue, value);
    PairNode* newPair = initPairNode(storeKey, storeValue);
    int tid = pthread_self();
    KeySet* keyset = findKeySet(tid);

    /* insert to keyset */
    if (keyset->keyHead == NULL) { // emptey keyset
        // new unique key 
        KeyNode* newKeyNode = initKeyNode(headKey, newPair);
        keyset->keyHead = newKeyNode;
        newKeyNode->pairHead = newPair;
        keyset->keyNum = 1;
        return;
    } else { // populated keyset
        // traverse through keyset
        KeyNode* curKeyNode = keyset->keyHead;
        for(int i = 0; i < keyset->keyNum; i++) {
            if (strcmp(key, curKeyNode->key) == 0) { // unique key exists
                //update pair head of this keyNode
                newPair->nextPair = curKeyNode->pairHead;
                curKeyNode->pairHead = newPair;
                return;
            } else {
                curKeyNode = curKeyNode->nextKeyNode;
            }
        }
        // after traversal, no existing unique key is found. Add new unique key
        KeyNode* newKeyNode = initKeyNode(headKey, newPair);
        newKeyNode->nextKeyNode = keyset->keyHead;
        keyset->keyHead = newKeyNode;
        newKeyNode->pairHead = newPair;
        keyset->keyNum += 1;
        return;
    }
}

void MR_EmitToReducer(char *key, char *value){
    // printf("IN MT EMIT TO REDUCE\n"); //DELETE THIS
    if (strcmp(key, "") == 0) { // TODO: delete this
        // return;
    }
    // assert(strcmp(key, "")); //DELETE THIS
    /* Adding to hTable */
    unsigned long partitionIndex;
    if (_partitioner == NULL) {
        partitionIndex = MR_DefaultHashPartition(key, _numPartitions);
    } else {
        partitionIndex = _partitioner(key, _numPartitions);
    }

    // Remember to free these two malloced spaces in each pairNode
    // Otherwise there will be direct lost in memory
    char* storeKey = malloc(strlen(key) + 1);
    char* storeValue = malloc(strlen(value) + 1);
    strcpy(storeKey, key);
    strcpy(storeValue, value);

    pthread_mutex_lock(&(hTable_[partitionIndex]->partitionLock));

    // Start of critical section, making sure there is only one mapper writing
    // append to partition list
    PairNode* newPair = initPairNode(storeKey, storeValue);
    newPair->nextPair = hTable_[partitionIndex]->pairHead;
    hTable_[partitionIndex]->pairHead = newPair;
    // assert(strcmp(newPair->key, "")); //DELETE THIS PROBLEM HERE
    // end of critical section
    pthread_mutex_unlock(&(hTable_[partitionIndex]->partitionLock));

    // FIXME: what if combiner is NULL? 
    /* Deleting from tTable_ */
    if (_combiner != NULL) { 
        // assuming combiner is finished with all pairNodes attached to this unique key
        pthread_t tid = pthread_self();
        KeySet* keyset = findKeySet(tid);
        KeyNode* keyHead = (KeyNode*)(keyset->keyHead); //FIXME: problem here?
        if (strcmp(key, keyHead->key) == 0) { // should emitted keyHead
            // free list behind this key
            PairNode* temp;
            while(keyHead->pairHead != NULL) {
                temp = keyHead->nextKeyNode->pairHead;
                free(keyHead->pairHead->key);
                free(keyHead->pairHead->value);
                free(keyHead->pairHead);
                keyHead->pairHead = temp;
            }
            // remove key head
            keyset->keyHead = keyset->keyHead->nextKeyNode;
            keyset->keyNum -= 1;
        } else {
            printf("LOGIC ERROR at EmitToReducer"); //FIXME
        }
    }
}
/*
void MR_Emit(char* key, char* value) {
    unsigned long partitionIndex;
    if (_partitioner == NULL) {
        partitionIndex = MR_DefaultHashPartition(key, _numPartitions);
    } else {
        partitionIndex = _partitioner(key, _numPartitions);
    }

    // Remember to free these two malloced spaces in each pairNode
    // Otherwise there will be direct lost in memory
    char* storeKey = malloc(strlen(key) + 1);
    char* storeValue = malloc(strlen(value) + 1);
    strcpy(storeKey, key);
    strcpy(storeValue, value);

    pthread_mutex_lock(&(hTable_[partitionIndex]->partitionLock));

    // Start of critical section, making sure there is only one mapper writing
    // append to partition list
    PairNode* newPair = initPairNode(storeKey, storeValue);
    newPair->nextPair = hTable_[partitionIndex]->pairHead;
    hTable_[partitionIndex]->pairHead = newPair;
    // end of critical section

    pthread_mutex_unlock(&(hTable_[partitionIndex]->partitionLock));
}
*/


char* combine_get_next(char* key) {
    pthread_t tid = pthread_self();
    KeySet* keyset = findKeySet(tid);
    if (keyset == NULL) {
        printf("LOGIC ERROR in combine_get_next, 1"); //FIXME
        return NULL;
    } 
    if (strcmp(key, keyset->keyHead->key) != 0) {
        printf("LOGIC ERROR in combine_get_next, 2"); //FIXME
        return NULL;
    }
    if (keyset->keyHead->pairHead == NULL) {
        return NULL; // all pairNodes under this key has depleted
    } else {
        // pop a node
        PairNode* temp = keyset->keyHead->pairHead;
        keyset->keyHead->pairHead = keyset->keyHead->pairHead->nextPair;
        return temp->value;
    }
}

void combinerCaller(KeySet *keyset) {
    // invoke combiner once per unique key
    for (int keyID = 0; keyID < keyset->keyNum; keyID++) {
        //FIXME: confirm last combine call updates keyhead
        (*_combiner)(keyset->keyHead->key, combine_get_next); 
    }
}



void* findFile() {
    printf("IN FIND FILE\n"); //DELETE THIS
    for (;;) {
        // printf("FF: total = %i, cur = %i\n", _totalFiles, _curFile); // DELETE THIS
        pthread_mutex_lock(&_mapLock);

        // Start of critical section, making sure each file goes to one mapper
        if (_curFile >= _totalFiles) { // FIXME: was >=
            pthread_mutex_unlock(&_mapLock);
            return NULL;
        }
        int target = _curFile;
        _curFile++;
        // End of critical section

        pthread_mutex_unlock(&_mapLock);

        // initalize keyset
        KeySet* keyset = initKeySet(pthread_self());
        insertKeySet(keyset);

        // printf("CALLING MAPPER\n\n"); //DELETE THIS

        // run mappers
        (*_mapper)(_argv[target]); 

        // run combiners
        combinerCaller(keyset);
    }
}



// Credit to https://www.geeksforgeeks.org/merge-sort-for-linked-list/
// ------------------------------------------------------------------------
PairNode* SortedMerge(PairNode* a, PairNode* b) {
    PairNode* result = NULL;

    /* Base cases */
    if (a == NULL) return b;
    if (b == NULL) return a;

    if (a->key == NULL || b->key == NULL)
        assert(0);

    /* Pick either a or b, and recur */
    if (strncmp(a->key, b->key, strlen(a->key)) <= 0) {
        result = a;
        result->nextPair = SortedMerge(a->nextPair, b);
    } else {
        result = b;
        result->nextPair = SortedMerge(a, b->nextPair);
    }
    return result;
}

void FrontBackSplit(PairNode* source, PairNode** frontRef, PairNode** backRef) {
    PairNode* fast;
    PairNode* slow;
    slow = source;
    fast = source->nextPair;

    /* Advance 'fast' two nodes, and advance 'slow' one node */
    while (fast != NULL) {
        fast = fast->nextPair;
        if (fast != NULL) {
            slow = slow->nextPair;
            fast = fast->nextPair;
        }
    }

    /* 'slow' is before the midpoint in the list, so split it in two
    at that point. */
    *frontRef = source;
    *backRef = slow->nextPair;
    slow->nextPair = NULL;
}

void Mergesort(PairNode** root) {
    PairNode* head = *root;
    PairNode* a;
    PairNode* b;

    /* Base case -- length 0 or 1 */
    if ((head == NULL) || (head->nextPair == NULL)) {
        return;
    }

    /* Split head into 'a' and 'b' sublists */
    FrontBackSplit(head, &a, &b);

    /* Recursively sort the sublists */
    Mergesort(&a);
    Mergesort(&b);

    /* answer = merge the two sorted lists together */
    *root = SortedMerge(a, b);
}
// ------------------------------------------------------------------------

/* Reducing Phase */
char* reduce_get_next(char* key, int partition_number) {
    if (hTable_[partition_number]->pairHead == NULL) {
        return NULL;
    }
    if (strcmp(hTable_[partition_number]->pairHead->key, key) == 0) {
        // switch the node we should reduce on to the backup list
        PairNode* temp = hTable_[partition_number]->pairHead;
        hTable_[partition_number]->pairHead =
            hTable_[partition_number]->pairHead->nextPair;
        temp->nextPair = hTable_[partition_number]->backUpHead;
        hTable_[partition_number]->backUpHead = temp;
        return hTable_[partition_number]->backUpHead->value;
    } else {
        return NULL;
    }
}

void reduceCaller(int partition_number) {
    printf("IN REDUCE CALLER\n"); // DELETE THIS
    PartitionNode* workingSpace = hTable_[partition_number];

    if (workingSpace->pairHead == NULL) {
        return;
    }

    if (workingSpace->lastKey == NULL) {
        workingSpace->lastKey = workingSpace->pairHead->key;
    }
    while (workingSpace->pairHead != NULL) {
        // printf("CALLING REDUCE\n"); //DELETE THIS
        (*_reducer)(workingSpace->lastKey, NULL, reduce_get_next, partition_number);
        if (workingSpace->pairHead != NULL)
            workingSpace->lastKey = workingSpace->pairHead->key;
    }
}

void* reduceHelper() {
    printf("PRE LOOP: num = %i, cur = %i\n", _numPartitions, _curPartition); // DELETE THIS
    for (;;) {
        pthread_mutex_lock(&_reduceLock);

        // Start of critical section, make sure each reducer gets one partition
        if (_curPartition >= _numPartitions) {
            pthread_mutex_unlock(&_reduceLock);
            return NULL;
        }
        int target = _curPartition;
        _curPartition++;
        // End of critical section

        pthread_mutex_unlock(&_reduceLock);
        reduceCaller(target);
    }
}

int compare(const void *a, const void *b) {
    return strcmp((*(struct PairNode **) a)->key,
                  (*(struct PairNode **) b)->key);
}

PairNode* sortNodes(PairNode* pairHead) {
    // counting size of nodes
    size_t size= 0;
    PairNode* cur = pairHead;
    while(cur!= NULL) {
        size += 1;
        if (cur->nextPair != NULL)
            cur = cur->nextPair;
        else
            break;
    }
    // convert list to array
    struct PairNode* arr[size];
    int i = 0;
    cur = pairHead;
    while(cur != NULL) {
        arr[i] = malloc(sizeof(PairNode*));
        arr[i] = cur;
        i++;
        cur = cur->nextPair;
    }
    // sort
    qsort(arr, size, sizeof(PairNode*), compare);
    //convert to list
    for (int i = 0 ; i < size-1; i++) { // skip head
        arr[i]->nextPair = arr[i+1];
    }
    arr[size-1]->nextPair = NULL;
    return arr[0];
}


void MR_Run(int argc, char* argv[], Mapper map, int num_mappers, Reducer reduce,
            int num_reducers, Combiner combine, Partitioner partition) {
    _mapper = map;
    _combiner = combine;
    _reducer = reduce;
    _partitioner = partition;
    _numMappers = num_mappers;
    _numPartitions = num_reducers; // FIXME
    _totalFiles = argc;
    _argv = argv;
    _curPartition = 0;
    _curFile = 1;

    printf("num = %i\n", _numPartitions); // DELETE THIS

    initTable();
    initThreadTable();

    int rc = pthread_mutex_init(&_mapLock, NULL);
    assert(!rc);
    rc = pthread_mutex_init(&_reduceLock, NULL);
    assert(!rc);

    pthread_t* mapperThreads = malloc(sizeof(pthread_t) * num_mappers);
    pthread_t* reduceThreads = malloc(sizeof(pthread_t) * num_reducers);

    for (int i = 0; i < num_mappers; i++) {
        pthread_create(&mapperThreads[i], NULL, &findFile, NULL);
    }
    printf("PRE JOIN\n"); //DELETE THIS

    for (int i = 0; i < num_mappers; i++) {
        pthread_join(mapperThreads[i], NULL);
    }

    printf("AFTER JOIN\n"); //DELETE THIS

    freeThreadTable();
    // Sort each partition before reducing
    for (int i = 0; i < num_reducers; i++) { // FIXME: was num_partitions
        // Mergesort(&(hTable_[i]->pairHead));
        printf("USING NEW SORT\n");
        sortNodes(hTable_[i]->pairHead);
        
    }

    // Done mapping, start reducing
    for (int i = 0; i < num_reducers; i++) {
        pthread_create(&reduceThreads[i], NULL, &reduceHelper, NULL);
    }
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reduceThreads[i], NULL);
    }

    // free and lock destruction
    freeTable();
    // freeThreadTable();
    free(mapperThreads);
    free(reduceThreads);
    pthread_mutex_destroy(&_mapLock);
    pthread_mutex_destroy(&_reduceLock);
}