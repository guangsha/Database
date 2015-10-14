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
void LogMgr::analyze(vector <LogRecord*> log){
    int tx_id = 0, lsn = 0, page_id = 0;
    auto it = log.rbegin();
    int start_lsn = NULL_LSN;    
    // search start LSN
    while(it != log.rend()){
        if((*it)->getType() == END_CKPT){
            ChkptLogRecord * ptr = dynamic_cast<ChkptLogRecord *>(*(it));
            tx_table = ptr->getTxTable();
            dirty_page_table = ptr->getDirtyPageTable();
            start_lsn = ptr->getLSN();
            break;
        }
        ++it; 
    }

    auto log_it = log.begin();
    
    if( (*log_it)->getLSN() < start_lsn || start_lsn != NULL_LSN){
        while(log_it != log.end()){
            if((*log_it)->getLSN() == start_lsn){
                break;     
            } 
            ++log_it;
        }
    }
    while(log_it != log.end()){
        LogRecord * log_ptr = *log_it;
        TxType type = log_ptr->getType();
        tx_id = log_ptr->getTxID();
        lsn = log_ptr->getLSN();
        setLastLSN(tx_id, lsn);
        if(tx_id != NULL_TX){
            tx_table[tx_id].status = U;
        }
        //TODO: switch case
        if(type == COMMIT){
            if(tx_id != NULL_TX){
                tx_table[tx_id].status = C;
            }
            
        } else if (type == UPDATE){
            UpdateLogRecord * u_ptr = dynamic_cast<UpdateLogRecord *>(log_ptr);
            page_id = u_ptr->getPageID();
            if(dirty_page_table.find(page_id) == dirty_page_table.end()){
                dirty_page_table[page_id] = lsn;   
            }      
        } else if (type == CLR) {
            CompensationLogRecord * u_ptr = dynamic_cast<CompensationLogRecord *>(log_ptr);
            page_id = u_ptr->getPageID();
            if(dirty_page_table.find(page_id) == dirty_page_table.end()){
                dirty_page_table[page_id] = lsn;   
            }  
        } else if (type == END){
            tx_table.erase(tx_id); 
        }
        ++log_it;
    }
}


/*
 * Run the redo phase of ARIES.
 * If the StorageEngine stops responding, return false.
 * Else when redo phase is complete, return true. 
 */

// start from the log recrod that has the smallst recLSN of all pages in the dirty page table
bool LogMgr::redo(vector <LogRecord*> log){
    if(!(dirty_page_table.empty())){
        int min_lsn = NULL_LSN;
        for(auto it = dirty_page_table.begin(); it != dirty_page_table.end(); ++it){
            if(it->second < min_lsn || min_lsn == NULL_LSN){
                min_lsn = it->second;    
            }    
        }
        
        auto log_it = log.begin();
        int page_id = 0, offset = 0, lsn = 0;
        string after_image = "";
        LogRecord * log_ptr = *log_it;
        
        // search start
        if(min_lsn != NULL_LSN && log_ptr->getLSN() < min_lsn){
            while(log_it != log.end()){
                log_ptr = *log_it;
                if(log_ptr->getLSN() == min_lsn){
                    break;    
                }
                ++log_it;
            }
        }
        
        while(log_it != log.end()){ 
            log_ptr = *log_it;
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
                ++log_it;
                continue;    
            }
            auto p_it = dirty_page_table.find(page_id);
            if(p_it == dirty_page_table.end()){
                ++log_it;
                continue; 
            }
            if(p_it->second > lsn){
                ++log_it;
                continue;
            }
            if(se->getLSN(page_id) >= lsn){
                ++log_it;
                continue;
            } 
            if(!(se->pageWrite(page_id, offset, after_image, lsn))){
                return false;
            } 
            ++log_it;
        }
    }
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
void LogMgr::undo(vector <LogRecord*> log, int txnum){
    priority_queue<int> ToUndo;
    LogRecord * log_rec_ptr = nullptr;
    int lsn = 0;
    string before_image = "";
    
    if(txnum == NULL_TX){
        for(auto it = tx_table.begin(); it != tx_table.end(); ++it){
            ToUndo.push(it->second.lastLSN); 
        }
    } else { 
        if(tx_table.find(txnum) != tx_table.end()){
            ToUndo.push(tx_table[txnum].lastLSN);    
        } 

    }
    bool found = false;
    while(!(ToUndo.empty())){
        found = false;
        lsn = ToUndo.top();
        ToUndo.pop();
        
        
        for(auto log_it = log.begin(); log_it != log.end(); ++log_it ){
            if((*log_it)->getLSN() == lsn){
                log_rec_ptr = *log_it;
                found = true;
                break; 
            }  
        }

        if(!found){
            continue;    
        }
        if(log_rec_ptr->getType() == UPDATE){
            UpdateLogRecord * u_ptr = dynamic_cast<UpdateLogRecord *>(log_rec_ptr);
            int new_lsn = se->nextLSN();
            
             
           
            logtail.push_back(new CompensationLogRecord(new_lsn, getLastLSN(u_ptr->getTxID()), u_ptr->getTxID(), 
                       u_ptr->getPageID(), u_ptr->getOffset(),u_ptr->getBeforeImage(), u_ptr->getprevLSN()));
            
            
            setLastLSN(u_ptr->getTxID(), new_lsn);
            if(u_ptr->getTxID() != NULL_TX){
                tx_table[u_ptr->getTxID()].status = U; 
            } 
            
            if(dirty_page_table.find(u_ptr->getPageID()) == dirty_page_table.end()){
                dirty_page_table[u_ptr->getPageID()] = new_lsn;    
            } 
            
            
            if(!(se->pageWrite(u_ptr->getPageID(), u_ptr->getOffset(), u_ptr->getBeforeImage(), new_lsn))){
                return;
            } 

            
            if(u_ptr->getprevLSN() != NULL_LSN){
                ToUndo.push(u_ptr->getprevLSN());
            } else {
               logtail.push_back(new LogRecord(se->nextLSN(), new_lsn, u_ptr->getTxID(), END));
               tx_table.erase(u_ptr->getTxID());
            }
        } else if (log_rec_ptr->getType() == CLR){
            CompensationLogRecord * c_ptr = dynamic_cast<CompensationLogRecord *>(log_rec_ptr);
            if(c_ptr->getUndoNextLSN() != NULL_LSN){
                ToUndo.push(c_ptr->getUndoNextLSN());   
            } else {
                logtail.push_back(new LogRecord(se->nextLSN(), c_ptr->getLSN(), c_ptr->getTxID(), END));
                tx_table.erase(c_ptr->getTxID());
            }
        } else if (log_rec_ptr->getType() == ABORT){
            if(log_rec_ptr->getprevLSN() != NULL_LSN){
                ToUndo.push(log_rec_ptr->getprevLSN());   
            } else {
                logtail.push_back(new LogRecord(se->nextLSN(), getLastLSN(log_rec_ptr->getTxID()), log_rec_ptr->getTxID(), END));
                tx_table.erase(log_rec_ptr->getTxID());
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
void LogMgr::abort(int txid){
    int last_lsn = getLastLSN(txid);
    int lsn = se->nextLSN();
    logtail.push_back(new LogRecord(lsn, last_lsn, txid, ABORT));
    setLastLSN(txid, lsn);
    if(txid != NULL_TX){
        tx_table[txid].status = U;
    }
    string log = se->getLog();    
    vector <LogRecord*> log_vec = stringToLRVector(log);
    for(auto log_ptr : logtail){
        log_vec.push_back(log_ptr);
    }
    undo(log_vec, txid); 
    
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

