/*
 * LogMgr.cpp
 */
 
#include "LogMgr.h"
#include <queue>
#include <cassert>
#include <sstream>
#include <list>

using namespace std;
/*
 * Find the LSN of the most recent log record for this TX.
 * If there is no previous log record for this TX, return 
 * the null LSN.
 */

int LogMgr::getLastLSN(int txnum){
    auto it = tx_table.find(txnum);
    if(it != tx_table.end()){
        return it->second.lastLSN;
    } else {
        return NULL_LSN;     
    }
}


/*
 * Update the TX table to reflect the LSN of the most recent
 * log entry for this transaction.
 */

void LogMgr::setLastLSN(int txnum, int lsn){
    if(txnum == NULL_TX){
        return;     
    }
    tx_table[txnum].lastLSN = lsn;
}

/*
 * Force log records up to and including the one with the
 * maxLSN to disk. Don't forget to remove them from the
 * logtail once they're written!
 */

void LogMgr::flushLogTail(int maxLSN){
    string log_string = "";
    while(!logtail.empty()){
        if(logtail.front()->getLSN() <= maxLSN){
            log_string.append(logtail.front()->toString());
            delete logtail.front();
            logtail.erase(logtail.begin());
        } else {
            break;    
        }     
    }               
    se->updateLog(log_string);
}


/* 
 * Run the analysis phase of ARIES.
 */
/* Start from the most recent check point
 *  initialize the tx_table and dirty page table
 */
void LogMgr::analyze(vector <LogRecord*> log)
{
  auto it = log.end();  
  TxType tType;
  int lsn, txID, pageID;
  bool foundCheckpoint = false;

  tx_table.clear();
  dirty_page_table.clear();

  if ( log.size() > 0 )
    {
      it--;
    }
  else
    {
      return;
    }

  while( it >= log.begin() )
    {
      LogRecord *logPointer = *it;
      tType = logPointer->getType();      
      if ( tType == END_CKPT )
        {
          ChkptLogRecord *chkptLogPointer = dynamic_cast<ChkptLogRecord *>(logPointer);
          tx_table = chkptLogPointer->getTxTable();
          dirty_page_table = chkptLogPointer->getDirtyPageTable();
          foundCheckpoint = true;
          break;
        }
      it--;
    }

  if ( !foundCheckpoint )
    {
      it = log.begin();
    }

  while( it != log.end() )
    {
      LogRecord *logPointer = *it;
      tType = logPointer->getType();
      txID = logPointer->getTxID();
      lsn = logPointer->getLSN();
      //tx_table[txID].lastLSN = lsn;
      setLastLSN(txID, lsn);
      
      if ( txID == NULL_TX )
        {
          it++;
          continue;
        }

      if ( tType ==  UPDATE)
        {
          UpdateLogRecord * updateLogPointer = dynamic_cast<UpdateLogRecord *>(logPointer);
          tx_table[txID].status = U;
          pageID = updateLogPointer->getPageID();
          if ( dirty_page_table.find(pageID) == dirty_page_table.end() )
            {
              // not found                                                                           
              dirty_page_table[pageID] = lsn; 
            }
        }
      else if( tType ==  COMMIT)
        {
          tx_table[txID].status = C;
        }
      else if( tType ==  CLR)
        {
          CompensationLogRecord * compensationLogPointer = dynamic_cast<CompensationLogRecord *>(logPointer);
          tx_table[txID].status = U;
          pageID = compensationLogPointer->getPageID();
          if ( dirty_page_table.find(pageID) == dirty_page_table.end() )
            {
              // not found
              dirty_page_table[pageID] = lsn;
            }
        }
      else if( tType ==  END)
        {
          tx_table.erase(txID);
        }
      it++;
    }
}


/*
 * Run the redo phase of ARIES.
 * If the StorageEngine stops responding, return false.
 * Else when redo phase is complete, return true. 
 */

// start from the log recrod that has the smallst recLSN of all pages in the dirty page table
bool LogMgr::redo(vector <LogRecord*> log)
{
  TxType tType;
  int lsn, pageID, offset, nextLsn;
  string afterImage;

  int oldestLsn = NULL_LSN;
  for ( auto it = dirty_page_table.begin(); it != dirty_page_table.end(); it++ )
    {
      if ( it->second < oldestLsn || oldestLsn == NULL_LSN )
	{
	  oldestLsn = it->second;
	}
    }

  auto log_it = log.begin();
  int page_id = 0;
  string after_image = "";
  LogRecord * log_ptr = *log_it;
  
  auto it = log.begin();
  if ( oldestLsn != NULL_LSN )
    {
      for (auto it = log.begin(); it != log.end(); it++)
	{
	  LogRecord *logPointer = *it;
	  lsn = logPointer->getLSN();
	  if ( lsn == oldestLsn )
	    {
	      break;
	    }
	}
    }

  //  for (auto it = log.begin(); it != log.end(); it++)
  for ( ; it != log.end(); it++)
    {
      LogRecord *logPointer = *it;
      tType = logPointer->getType();
      lsn = logPointer->getLSN();

      if ( tType == UPDATE ) 
        {
          UpdateLogRecord * updateLogPointer = dynamic_cast<UpdateLogRecord *>(logPointer);
          pageID = updateLogPointer->getPageID();
          afterImage = updateLogPointer->getAfterImage();
          offset = updateLogPointer->getOffset();
        }
      else if ( tType == CLR )
        {
          CompensationLogRecord * compensationLogPointer = dynamic_cast<CompensationLogRecord *>(logPointer);
          pageID = compensationLogPointer->getPageID();
          afterImage = compensationLogPointer->getAfterImage();
          offset = compensationLogPointer->getOffset();
        }      
      else
	{
	  continue;
	}
      
      if ( dirty_page_table.find(pageID) == dirty_page_table.end() )
        {
          continue;
        }
      if( dirty_page_table[pageID] > lsn || se->getLSN(pageID) >= lsn )
        {
	  continue;
	}
      if( !(se->pageWrite(pageID, offset, afterImage, lsn)) )
	{
	  return false;
	}
    }
  /*

  while(it != log.end()){ 
    log_ptr = *it;
    lsn = log_ptr->getLSN();
    //TODO: change to switch case statement
    if(log_ptr->getType() == UPDATE){
      UpdateLogRecord * u_ptr = dynamic_cast<UpdateLogRecord *>(log_ptr); 
      page_id =  u_ptr ->getPageID();
      after_image = u_ptr->getAfterImage();
      offset = u_ptr->getOffset();
    } else if (log_ptr->getType() == CLR){
      CompensationLogRecord * c_ptr = dynamic_cast<CompensationLogRecord *>(log_ptr);
      page_id =  c_ptr->getPageID();
      after_image = c_ptr->getAfterImage();
      offset = c_ptr->getOffset();
    } else {
      ++it;
      continue;    
    }
    auto p_it = dirty_page_table.find(page_id);
    if(p_it == dirty_page_table.end()){
      ++it;
      continue; 
    }
    if(p_it->second > lsn){
      ++it;
      continue;
    }
    if(se->getLSN(page_id) >= lsn){
      ++it;
      continue;
    } 
    if(!(se->pageWrite(page_id, offset, after_image, lsn))){
      return false;
    } 
    ++it;
  }
  */

  TxStatus status;
  list<int> to_delete_list;
  for(auto t_it = tx_table.begin(); t_it != tx_table.end(); ++t_it){
    status = t_it->second.status;
    if(status == C){
      to_delete_list.push_back(t_it->first);
      logtail.push_back(new LogRecord(se->nextLSN(), t_it->second.lastLSN, t_it->first, END));
    }
  }
  for(auto ele : to_delete_list){
    tx_table.erase(ele);
  }

  return true;
}

/*
 * If no txnum is specified, run the undo phase of ARIES.
 * If a txnum is provided, abort that transaction.
 * Hint: the logic is very similar for these two tasks!
 */

// scan backwards from the log
void LogMgr::undo(vector <LogRecord*> log, int txnum)
{
  vector <int> loserTxID;
  priority_queue <int> ToUndo;
  int lsn, lastLsn, nextLsn, txID, pageID, offset, prevLsn, undoNextLsn, toUndoLsn;
  TxType tType;
  string beforeImage;

  // If a txnum is provided, abort that transaction.
  if ( txnum != NULL_TX )
    {
      lsn = se->nextLSN();
      lastLsn = getLastLSN(txnum);
      setLastLSN(txnum, lsn);
      logtail.push_back(new LogRecord(lsn, lastLsn, txnum, ABORT));
      log.push_back(new LogRecord(lsn, lastLsn, txnum, ABORT));
      tx_table[txnum].lastLSN = lsn;
      tx_table[txnum].status = U;
      loserTxID.push_back(txnum);
    }
  else
    {
      for (auto it = tx_table.begin(); it != tx_table.end(); it++)
        {
          if ( it->second.status != C )
            {
              loserTxID.push_back(it->first);
            }
        }
    }

  for ( int i = 0; i < loserTxID.size(); i++ )
    {
      ToUndo.push(tx_table[loserTxID[i]].lastLSN);
    }

  LogRecord *logPointer;
  while ( !(ToUndo.empty()) )
    //  while ( it >= log.begin() && !(ToUndo.empty()) )
    {
      toUndoLsn = ToUndo.top();
      ToUndo.pop();
      bool foundLsn = false;
      
      for ( auto it = log.begin(); it != log.end(); it++ )
	{
	  logPointer = *it;
	  lsn = logPointer->getLSN();
	  if ( toUndoLsn == lsn )
	    {
	      foundLsn = true;
	      break;
	    }
	}
      
      if ( !foundLsn )
	{
	  continue;
	}

      tType = logPointer->getType();
      
      if ( tType == UPDATE )
	{
	  UpdateLogRecord * updateLogPointer = dynamic_cast<UpdateLogRecord *>(logPointer);
	  txID = updateLogPointer->getTxID();
	  pageID = updateLogPointer->getPageID();
	  offset = updateLogPointer->getOffset();
	  beforeImage = updateLogPointer->getBeforeImage();
	  prevLsn = updateLogPointer->getprevLSN();
	  lastLsn = getLastLSN(txID);
	  nextLsn = se->nextLSN();
	  logtail.push_back(new CompensationLogRecord(nextLsn, lastLsn, txID, pageID, offset, beforeImage, prevLsn));
	  setLastLSN(txID, nextLsn);
	  tx_table[txID].status = U;
          
	  if ( dirty_page_table.find(pageID) == dirty_page_table.end() )
	    {
	      dirty_page_table[pageID] = nextLsn; 
	    }
	  if( !(se->pageWrite(pageID, offset, beforeImage, nextLsn)) )
	    {
	      return;
	    }
	  if ( prevLsn == NULL_LSN )
	    {
	      logtail.push_back( new LogRecord(se->nextLSN(), nextLsn, txID, END) );
	      tx_table.erase(txID);
	    }
	  else
	    {
	      ToUndo.push(prevLsn);
	    }
	}
      else if ( tType == CLR )
	{
	  CompensationLogRecord* compensationLogPointer = dynamic_cast<CompensationLogRecord*>(logPointer);
	  undoNextLsn = compensationLogPointer->getUndoNextLSN();
	  if(undoNextLsn != NULL_LSN)
	    {
	      ToUndo.push(undoNextLsn);   
	    } 
	  else 
	    {
	      txID = compensationLogPointer->getTxID();
	      nextLsn = se->nextLSN();
	      logtail.push_back( new LogRecord(nextLsn, lsn, txID, END) );
	      tx_table.erase(txID);
	    }
	}
      
      else if ( tType == ABORT )
	{
	  if(prevLsn != NULL_LSN)
	    {
	      prevLsn = logPointer->getprevLSN();
	      ToUndo.push(prevLsn);
	    }
	  else
	    {
	      txID = logPointer->getTxID();
	      nextLsn = se->nextLSN();
	      logtail.push_back( new LogRecord(nextLsn, lsn, txID, END) );
	      tx_table.erase(txID);
	    }
	}
    }
}




vector<LogRecord*> LogMgr::stringToLRVector(string logstring){

    stringstream stream(logstring); 
    string line;
    vector<LogRecord*> result; 
    while (getline(stream, line)) { 
        LogRecord* lr = LogRecord::stringToRecordPtr(line); 
        result.push_back(lr); 
    } 
    return result; 
}

// an bort type log record is appended 
// undo is initiated


/*
 * Abort the specified transaction.
 * Hint: you can use your undo function
 */
void LogMgr::abort(int txid)
{
  string logString = se->getLog();
  vector <LogRecord*> log = stringToLRVector(logString);
  log.insert( log.end(), logtail.begin(), logtail.end() );
  undo(log, txid);
}


/*
 * Write the begin checkpoint and end checkpoint
 */

/* Step1: begin_checkpoint record is written
 * Step2: end_checkpoint record is constructed, include it in the current contents of 
 *        tx_table, dirty_page_table and append to log
 * Step3: after end_checkpoint is written:
 *        - special master record containing the LSN of the 
 *       begin_checkpoint record is written to a known place in the disk
 */

void LogMgr::checkpoint(){
    int begin_lsn = se->nextLSN();
    int end_lsn = se->nextLSN();
    logtail.push_back(new LogRecord(begin_lsn, NULL_LSN, NULL_TX, BEGIN_CKPT));
    logtail.push_back(new ChkptLogRecord(end_lsn, begin_lsn, NULL_TX, tx_table, dirty_page_table));
    flushLogTail(end_lsn);
    se->store_master(begin_lsn);
}


/*
 * Commit the specified transaction.
 */


void LogMgr::commit(int txid){
    int last_lsn = getLastLSN(txid);
    int commit_lsn = se->nextLSN();
    logtail.push_back(new LogRecord(commit_lsn, last_lsn, txid, COMMIT));
    setLastLSN(txid, commit_lsn);
    tx_table[txid].status = C;    

    flushLogTail(commit_lsn);
    tx_table.erase(txid);
    int end_lsn = se->nextLSN();
    logtail.push_back(new LogRecord(end_lsn, commit_lsn, txid, END));
}


/*
 * A function that StorageEngine will call when it's about to 
 * write a page to disk. 
 * Remember, you need to implement write-ahead logging
 */

void LogMgr::pageFlushed(int page_id){
   int lsn = se->getLSN(page_id);
   flushLogTail(lsn);
   dirty_page_table.erase(page_id); 
}

/*
 * Recover from a crash, given the log from the disk.
 */

void LogMgr::recover(string log){
    vector<LogRecord*> log_vect = stringToLRVector(log);
    analyze(log_vect); 
    if(redo(log_vect)){
        undo(log_vect);
    }
}




/*
 * Logs an update to the database and updates tables if needed.
 */
/*
 * That log created by write() is in the logtail,  and should be flushed to disk before moving data to disk.
 * should return the LSN of current write
 */ 
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
    int lsn = se->nextLSN();
    int prev_lsn = getLastLSN(txid);
    logtail.push_back(new UpdateLogRecord(lsn, prev_lsn, txid, page_id, offset, oldtext, input));
    setLastLSN(txid, lsn); 
    tx_table[txid].status = U;
    auto it = dirty_page_table.find(page_id);
    if(it == dirty_page_table.end()){
        dirty_page_table[page_id] = lsn;    
    }
    return lsn;
}


/*
 * Sets this.se to engine. 
 */

void LogMgr::setStorageEngine(StorageEngine* engine){
    se = engine;
}

