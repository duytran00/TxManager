/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>

//Modified at 6:35 PM 09/29/2022 by Jay D. Bodra.
// Fall 2016[jay]. Removed the TxType. Now is it initialized once in the constructor

extern void *start_operation(long, long);  //start an op with mutex lock and cond wait
extern void *finish_operation(long);        //finish an op with mutex unlock and con signal

extern void *do_commit_abort(long, char);   //commit/abort based on char value
extern void *process_read_write(long, long, int, char);

extern zgt_tm *ZGT_Sh;			// Transaction manager object

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

//Fall 2016[jay]. Modified zgt_tx() in zgt_tx.h
zgt_tx::zgt_tx( long tid, char Txstatus,char type, pthread_t thrid){
  this->lockmode = (char)' ';  //default
  this->Txtype = type; //Fall 2022[jay] R = read only, W=Read or Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1; //set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1; //init to  an invalid sem value
  this->waiters = 0;
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is NOT empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list returns NULL
   }
  return(NULL);				// if list is empty returns NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //intialise a transaction object. Make sure it is 
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. when creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
  
  struct param *node = (struct param*)arg;// get tid and count
  start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node
 
    //Fall 2016[jay]. writes the Txtype to the file.
  
    zgt_p(0);				// Lock Tx manager; Add node to transaction list
  
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;   
    zgt_v(0); 			// Release tx manager 
  fprintf(ZGT_Sh->logfile, "T%ld\t%c \tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
    fflush(ZGT_Sh->logfile);
  finish_operation(node->tid);
  pthread_exit(NULL);				// thread exit
}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg){
  // "The return types are void*."

  // "the required parameters of the function like
  // transaction id, object number etc. need to be passed in a structure"
  struct param *node = (struct param*)arg;// get tid and objno and count
  // "The thread has to make sure that a previous operation by the same transaction
  // has been completed before starting the current operation"; start_operation
  // does the cond_wait on condset[tid] for that
  start_operation(node->tid, node->count);

  // An object number outside objarray would read past the end of the array
  if ((node->obno < 0) || (node->obno >= MAX_ITEMS)){
    printf(":::ERROR: ReadTx by Tx:%ld on object:%ld which is out of range (0 to %d)\n",
        node->tid, node->obno, MAX_ITEMS - 1);
    fflush(stdout);
    fprintf(ZGT_Sh->logfile, ":::ERROR: ReadTx by Tx:%ld on object:%ld which is out of range (0 to %d)\n",
        node->tid, node->obno, MAX_ITEMS - 1);
    fflush(ZGT_Sh->logfile);
  }
  else {
    // The Tx list is changed by other threads (begintx, commit, abort), so it is
    // searched while holding the TM semaphore 0; released before set_lock so this
    // thread never waits for an object while holding it
    zgt_p(0);
    zgt_tx *tx = get_tx(node->tid);
    zgt_v(0);

    if (tx == NULL){
      // "all errors and the log output need to be printed (or transmitted) at the
      // point of occurrence"
      printf(":::ERROR: ReadTx on Tx:%ld which does not exist\n", node->tid);
      fflush(stdout);
      fprintf(ZGT_Sh->logfile, ":::ERROR: ReadTx on Tx:%ld which does not exist\n", node->tid);
      fflush(ZGT_Sh->logfile);
    }
    // "...transaction inserts the object into the hash table in the appropriate
    // lock-mode." S = shared lock for a read; set_lock returns 0 when it is granted
    else if (tx->set_lock(node->tid, tx->sgno, node->obno, node->count, 'S') == 0){
      // If this Tx waited for the lock and was aborted, the read
      // must not be done. tx is still valid here: this Tx's own commit/abort cannot
      // start until this operation calls finish_operation.
      if (tx->status != TR_ABORT){
        tx->perform_readWrite(node->tid, node->obno, 'S');
      }
    }
  }

  // Let the next operation of this Tx proceed (decrements condset[tid] and broadcasts)
  finish_operation(node->tid);
  pthread_exit(NULL);
}


void *writetx(void *arg){ //do the operations for writing; similar to readTx
  struct param *node = (struct param*)arg;	// struct parameter that contains tid, obno and count
  //do the operations for writing; similar to readTx.
  // Wait until the previous operation of this Tx has finished (input order within a Tx)
  start_operation(node->tid, node->count);

  // An object number outside objarray would write past the end of the array
  if ((node->obno < 0) || (node->obno >= MAX_ITEMS)){
    printf(":::ERROR: WriteTx by Tx:%ld on object:%ld which is out of range (0 to %d)\n",
        node->tid, node->obno, MAX_ITEMS - 1);
    fflush(stdout);
    fprintf(ZGT_Sh->logfile, ":::ERROR: WriteTx by Tx:%ld on object:%ld which is out of range (0 to %d)\n",
        node->tid, node->obno, MAX_ITEMS - 1);
    fflush(ZGT_Sh->logfile);
  }
  else {
    // Search the shared Tx list under the TM semaphore 0, and release it before
    // set_lock so this thread never waits for an object while holding it
    zgt_p(0);
    zgt_tx *tx = get_tx(node->tid);
    zgt_v(0);

    if (tx == NULL){
      // Errors are printed where they happen; a thread cannot return an error status
      printf(":::ERROR: WriteTx on Tx:%ld which does not exist\n", node->tid);
      fflush(stdout);
      fprintf(ZGT_Sh->logfile, ":::ERROR: WriteTx on Tx:%ld which does not exist\n", node->tid);
      fflush(ZGT_Sh->logfile);
    }
    // X = exclusive lock for a write; set_lock returns 0 when it is granted
    else if (tx->set_lock(node->tid, tx->sgno, node->obno, node->count, 'X') == 0){
      // Skip the write if this Tx was aborted while it waited for the lock.
      // tx is still valid: this Tx's commit/abort waits for this operation to finish.
      if (tx->status != TR_ABORT){
        tx->perform_readWrite(node->tid, node->obno, 'X');
      }
    }
  }

  // Let the next operation of this Tx proceed
  finish_operation(node->tid);
  pthread_exit(NULL);
}

//common method to process read/write: just a useful suggestion

void *process_read_write(long tid, long obno,  int count, char mode){

  return(0);
}

void *aborttx(void *arg)
{
  struct param *node = (struct param*)arg;
  start_operation(node->tid, node->count);
  do_commit_abort(node->tid, TR_ABORT);
  finish_operation(node->tid);
  pthread_exit(NULL);
}

void *committx(void *arg)
{
  struct param *node = (struct param*)arg;
  start_operation(node->tid, node->count);
  do_commit_abort(node->tid, TR_END);
  finish_operation(node->tid);
  pthread_exit(NULL);			// thread exit
}

//suggestion as they are very similar

// called from commit/abort with appropriate parameter to do the actual
// operation. Make sure you give error messages if you are trying to
// commit/abort a non-existant tx

void *do_commit_abort(long t, char status){
  zgt_p(0);
  zgt_tx *tx = get_tx(t);
  if (tx == NULL){
    zgt_v(0);
    const char *operation = status == TR_ABORT ? "AbortTx" : "CommitTx";
    fprintf(ZGT_Sh->logfile, ":::ERROR: %s on Tx:%ld which does not exist\n", operation, t);
    fflush(ZGT_Sh->logfile);
    printf(":::ERROR: %s on Tx:%ld which does not exist\n", operation, t);
    fflush(stdout);
    return(NULL);
  }

  tx->status = status;
  const char *operation = status == TR_ABORT ? "AbortTx" : "CommitTx";
  fprintf(ZGT_Sh->logfile, "T%ld\t \t%s\t", t, operation);
  tx->free_locks();

  for (int waiter = 0; waiter < tx->waiters; ++waiter){
    zgt_v(tx->tid);
  }

  if (tx->remove_tx() == 0){
    delete tx;
  }
  zgt_v(0);
  return(NULL);
}

int zgt_tx::remove_tx ()
{
  //remove the transaction from the TM

  zgt_tx **transactionLink = &ZGT_Sh->lastr;
  while (*transactionLink && (*transactionLink)->tid != this->tid){
    transactionLink = &(*transactionLink)->nextr;
  }

  if (*transactionLink){
    *transactionLink = (*transactionLink)->nextr;
    return(0);
  }

  fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%ld that does not exist\n", this->tid);
  fflush(ZGT_Sh->logfile);
  printf("Trying to Remove a Tx:%ld that does not exist\n", this->tid);
  fflush(stdout);
  return(-1);
}

/* this method sets lock on objno1 with lockmode1 for a tx*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1){
  (void)count;

  if (lockmode1 != 'S' && lockmode1 != 'X'){
    fprintf(ZGT_Sh->logfile, ":::ERROR: Invalid lock mode %c for Tx:%ld\n", lockmode1, tid1);
    fflush(ZGT_Sh->logfile);
    printf(":::ERROR: Invalid lock mode %c for Tx:%ld\n", lockmode1, tid1);
    fflush(stdout);
    return(-1);
  }

  while (true){
    zgt_p(0);
    zgt_tx *requester = get_tx(tid1);
    if (requester == NULL || requester->status == TR_ABORT || requester->status == TR_END){
      zgt_v(0);
      return(-1);
    }

    zgt_hlink *ownLock = ZGT_Ht->findt(tid1, sgno1, obno1);
    if (ownLock != NULL){
      requester->status = TR_ACTIVE;
      requester->obno = -1;
      requester->lockmode = ' ';
      zgt_v(0);
      return(0);
    }

    zgt_hlink *heldLock = ZGT_Ht->find(sgno1, obno1);
    if (heldLock == NULL || (lockmode1 == 'S' && heldLock->lockmode == 'S')){
      if (ZGT_Ht->add(requester, sgno1, obno1, lockmode1) != 0){
        zgt_v(0);
        fprintf(ZGT_Sh->logfile, ":::ERROR: Unable to add lock for Tx:%ld on object:%ld\n", tid1, obno1);
        fflush(ZGT_Sh->logfile);
        printf(":::ERROR: Unable to add lock for Tx:%ld on object:%ld\n", tid1, obno1);
        fflush(stdout);
        return(-1);
      }

      requester->status = TR_ACTIVE;
      requester->obno = -1;
      requester->lockmode = ' ';
      zgt_v(0);
      return(0);
    }

    zgt_tx *holder = get_tx(heldLock->tid);
    if (holder == NULL){
      zgt_v(0);
      fprintf(ZGT_Sh->logfile, ":::ERROR: Lock holder Tx:%ld does not exist\n", heldLock->tid);
      fflush(ZGT_Sh->logfile);
      printf(":::ERROR: Lock holder Tx:%ld does not exist\n", heldLock->tid);
      fflush(stdout);
      return(-1);
    }

    requester->status = TR_WAIT;
    requester->obno = obno1;
    requester->lockmode = lockmode1;
    holder->semno = (int)holder->tid;
    holder->waiters++;
    int waitSem = holder->semno;
    zgt_v(0);

    zgt_p(waitSem);

    zgt_p(0);
    requester = get_tx(tid1);
    if (requester == NULL || requester->status == TR_ABORT || requester->status == TR_END){
      zgt_v(0);
      return(-1);
    }

    requester->status = TR_ACTIVE;
    requester->obno = -1;
    requester->lockmode = ' ';
    zgt_v(0);
  }
}

int zgt_tx::free_locks()
{
  
  // this part frees all locks owned by the transaction
  // that is, remove the objects from the hash table
  // and release all Tx's waiting on this Tx

  zgt_hlink *temp = head;
  bool firstLock = true;
  while(temp != NULL){
      zgt_hlink *next = temp->nextp;
      long obno = temp->obno;
      long tid = temp->tid;
      char lockmode = temp->lockmode;

      if (!firstLock) fprintf(ZGT_Sh->logfile, ", ");
      fprintf(ZGT_Sh->logfile, "%ld : %d", obno, ZGT_Sh->objarray[obno]->value);
      fflush(ZGT_Sh->logfile);
      firstLock = false;
      
      if (ZGT_Ht->remove(this, 1, obno) == 1){
        printf(":::ERROR:node with tid:%ld and onjno:%ld was not found for deleting", this->tid, obno);
        fflush(stdout);
      }
      else {
#ifdef TX_DEBUG
        printf("\n:::Hash node with Tid:%ld, obno:%ld lockmode:%c removed\n",
               tid, obno, lockmode);
        fflush(stdout);
#endif
      }
      temp = next;
    }
  fprintf(ZGT_Sh->logfile, "\n");
  fflush(ZGT_Sh->logfile);
  
  return(0);
}		

// CURREntly Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  //2016: not used
{
  zgt_tx *linktx, *prevp;
  
  // USED to COMMIT 
  //remove the transaction and free all associate dobjects. For the time being 
  //this can be used for commit of the transaction.
  
  linktx = prevp = ZGT_Sh->lastr;
  
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
  return(0);
}

// CURRENTLY not used
int zgt_tx::cleanup()
{
  return(0);
  
}

//initial implementation by  sharma chakravarthy as part of Sentinel-oodbms project 
// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print
void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx  list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%ld\t%c\t%ld\t%ld\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

//need to be called for printing
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
  return;
}

void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  return;
}

// routine to perform the acutual read/write operation
// based  on the lockmode

void zgt_tx::perform_readWrite(long tid,long obno, char lockmode){

  // tid = transaction id, obno = object index, lockmode = 'S' (shared, read)
  // or 'X' (exclusive, write).
  // Several read-only Txs can hold a shared lock on the same object at once, and a
  // read changes the value here, so the update and the log line that prints the new
  // value must not interleave with another thread's update of an object. This mutex
  // guards only the object values; it is held for the update and the log write and
  // released before the sleep.
  static pthread_mutex_t objectValueMutex = PTHREAD_MUTEX_INITIALIZER;

  pthread_mutex_lock(&objectValueMutex);
  // "The operation in our case is decrementing the object value by 1 and printing a
  // line in the log if it is a read operation"
  if (lockmode == 'S'){
    ZGT_Sh->objarray[obno]->value--;
    // this->status is the Tx status: TR_ACTIVE='P', TR_WAIT='W', TR_ABORT='A', TR_END='E'
    fprintf(ZGT_Sh->logfile, "T%ld\t      \tReadTx\t\t%ld:%d:%d\t\tReadLock\tGranted\t\t %c\n",
        tid, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
  }
  // "For a write operation, we increment the value by one, write this to the log"
  else {
    ZGT_Sh->objarray[obno]->value++;
    fprintf(ZGT_Sh->logfile, "T%ld\t      \tWriteTx\t\t%ld:%d:%d\t\tWriteLock\tGranted\t\t %c\n",
        tid, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
  }
  // "Do a flush on the logfile write operation to force write it."
  fflush(ZGT_Sh->logfile);
  pthread_mutex_unlock(&objectValueMutex);

  // "sleeping for the given time (optime) ... The sleep is to simulate disk
  // read/write operations"
  usleep(ZGT_Sh->optime[tid]);
}

// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %ld wants to wait on sem:%d of tid:%ld which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    exit(1);
  }
  if ((txptr->semno == -1)|| (txptr->semno == semno)){  //just to be safe
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%ld\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
  return(0);
}

// Otherside of teh start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
  return(0);
}


