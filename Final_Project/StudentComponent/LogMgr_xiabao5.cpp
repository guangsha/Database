#include "LogMgr.h"
#include <queue>
#include <sstream>

using namespace std;

/*
 * Find the LSN of the most recent log record for this TX.
 * If there is no previous log record for this TX, return 
 * the null LSN.
 */
int LogMgr::getLastLSN(int txnum)
{
  if ( tx_table.find(txnum) == tx_table.end() )
    {
      // not found
      return NULL_LSN;
    }
  else 
    {
      // found
      return tx_table[txnum].lastLSN;
    }
}


/*
 * Update the TX table to reflect the LSN of the most recent
 * log entry for this transaction.
 */
void LogMgr::setLastLSN(int txnum, int lsn)
{
  tx_table[txnum].lastLSN = lsn;
}


/*
 * Force log records up to and including the one with the
 * maxLSN to disk. Don't forget to remove them from the
 * logtail once they're written!
 */
void LogMgr::flushLogTail(int maxLSN)
{
  string logtoflush = "";
  auto it = logtail.begin();
  while(it != logtail.end())
    {
      if ( (*it)->getLSN() > maxLSN )
	{
	  break;
	}
      else
	{
	  logtoflush.append( (*it)->toString() );
	  it = logtail.erase(it);
	}
    }
  se->updateLog(logtoflush);
}


/* 
 * Run the analysis phase of ARIES.
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
      tx_table[txID].lastLSN = lsn;

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
bool LogMgr::redo(vector <LogRecord*> log)
{
  TxType tType;
  int lsn, pageID, offset, nextLsn;
  string afterImage;

  for(auto it = log.begin(); it != log.end(); it++)
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
      
      if ( dirty_page_table.find(pageID) == dirty_page_table.end() )
        {
          continue;
        }
      if( dirty_page_table[pageID] <= lsn && se->getLSN(pageID) < lsn )
        {
          if( !(se->pageWrite(pageID, offset, afterImage, lsn)) )
            {
              return false;
            }
        }
    }

  vector <int> txToErase;
  for ( auto it = tx_table.begin(); it != tx_table.end(); it++ )
    {
      if( it->second.status == C && it->first != NULL_TX )
        {
	  nextLsn = se->nextLSN();
          logtail.push_back(new LogRecord(nextLsn, it->second.lastLSN, it->first, END));
	  txToErase.push_back(it->first);
        }
    }
  for ( int i = 0; i < txToErase.size(); i++ )
    {
      tx_table.erase(txToErase[i]);
    }

  return true;
}


/*
 * If no txnum is specified, run the undo phase of ARIES.
 * If a txnum is provided, abort that transaction.
 * Hint: the logic is very similar for these two tasks!
 */
void LogMgr::undo(vector <LogRecord*> log, int txnum)
{
  vector <int> loserTxID;
  priority_queue <int> ToUndo;
  int lsn, lastLsn, nextLsn, txID, pageID, offset, prevLsn, undoNextLsn;
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

  auto it = log.end();
  if ( log.size() > 0 )
    {
      it--;
    }
  else
    {
      return;
    }
      
  for ( int i = 0; i < loserTxID.size(); i++ )
    {
      ToUndo.push(tx_table[loserTxID[i]].lastLSN);
    }

  it = log.end();
  if ( log.size() > 0 )
    {
      it--;
    }
  else
    {
      return;
    }
  while ( it >= log.begin() && !(ToUndo.empty()) )
    {
      LogRecord *logPointer = *it;
      tType = logPointer->getType();
      lsn = logPointer->getLSN();
      if (lsn == ToUndo.top())
        {
          ToUndo.pop();
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
      it--;
    }
}


vector<LogRecord*> LogMgr::stringToLRVector(string logstring)
{
  vector<LogRecord*> result;
  istringstream stream(logstring);
  string line;
  while (getline(stream, line)) 
    {
      LogRecord* lr = LogRecord::stringToRecordPtr(line);
      result.push_back(lr);
    }
  return result; 
}


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
void LogMgr::checkpoint()
{
  int beginCheckpointLsn = se->nextLSN();
  int endCheckpointLsn = se->nextLSN();
  // Step 1: Log a begin_checkpoint
  logtail.push_back(new LogRecord(beginCheckpointLsn, NULL_LSN, NULL_TX, BEGIN_CKPT));
  // Step 2: Log a end_checkpoint
  logtail.push_back(new ChkptLogRecord(endCheckpointLsn, beginCheckpointLsn, NULL_TX, tx_table, dirty_page_table));
  // Step 3: Flush the log tail
  flushLogTail(endCheckpointLsn);
  // Step 4: Store the begin checkpoint at the master
  se->store_master(beginCheckpointLsn);
}


/*
 * Commit the specified transaction.
 */
void LogMgr::commit(int txid)
{
  int lastLsn = getLastLSN(txid);
  int nextLsn = se->nextLSN();
  logtail.push_back( new LogRecord(nextLsn, lastLsn, txid, COMMIT) );
  setLastLSN(txid, nextLsn);
  tx_table.erase(txid);
  flushLogTail(nextLsn); // Write to the log tail to disk
  logtail.push_back( new LogRecord(se->nextLSN(), nextLsn, txid, END) );
}


/*
 * A function that StorageEngine will call when it's about to 
 * write a page to disk. 
 * Remember, you need to implement write-ahead logging
 */
void LogMgr::pageFlushed(int page_id)
{
  int maxlsn = se->getLSN(page_id);
  flushLogTail(maxlsn);
  dirty_page_table.erase(page_id);
}


/*
 * Recover from a crash, given the log from the disk.
 */
void LogMgr::recover(string log)
{
  vector<LogRecord*> logRec = stringToLRVector(log);
  analyze(logRec);
  if ( redo(logRec) )
    {
      undo(logRec);
    }
}

/*
 * Logs an update to the database and updates tables if needed.
 */
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext)
{
  int nextLsn = se->nextLSN();
  int lastLsn = getLastLSN(txid);
  logtail.push_back( new UpdateLogRecord(nextLsn, lastLsn, txid, page_id, offset, oldtext, input) );
  setLastLSN(txid, nextLsn);
  tx_table[txid].status = U;
  if( dirty_page_table.find(page_id) == dirty_page_table.end() )
    {
      dirty_page_table[page_id] = nextLsn;
    }
  return nextLsn;
}


/*                                                                                                                              
 * Sets this.se to engine.                                                                                                      
 */
void LogMgr::setStorageEngine(StorageEngine* engine)
{
  se = engine;
}
