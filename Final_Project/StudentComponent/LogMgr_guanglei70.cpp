/*
 * LogMgr.cpp
 */
 
#include "LogMgr.h"
#include <queue>
#include <cassert>
#include <sstream>
#include <climits>
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
    tx_table[txnum].lastLSN = lsn;
}

/*
 * Force log records up to and including the one with the
 * maxLSN to disk. Don't forget to remove them from the
 * logtail once they're written!
 */

void LogMgr::flushLogTail(int maxLSN){
    string log_string = "";
    auto it = logtail.begin();
    while(it != logtail.end()){
        if((*it)->getLSN() <= maxLSN){
            log_string.append((*it)->toString());
            it = logtail.erase(it);
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
    if((*it)->getType() == END_CKPT){
        ChkptLogRecord * ptr = dynamic_cast<ChkptLogRecord *>(*(it));
        tx_table = ptr->getTxTable();
        dirty_page_table = ptr->getDirtyPageTable();
        ++it;
    } else {
        tx_table.clear();
        dirty_page_table.clear();    
    } 
    while(it != log.rend()){
        LogRecord * log_it = *it;
        TxType type = log_it->getType();
        tx_id = log_it->getTxID();
        lsn = log_it->getLSN();
        tx_table[tx_id].lastLSN = lsn;
        //TODO: switch case
        if(type == COMMIT){
            tx_table[tx_id].status = C;
        } else if (type == UPDATE){
            tx_table[tx_id].status = U;
            UpdateLogRecord * u_ptr = dynamic_cast<UpdateLogRecord *>(log_it);
            page_id = u_ptr->getPageID();
            auto p_it = dirty_page_table.find(page_id);
            if(p_it == dirty_page_table.end()){
                dirty_page_table[page_id] = lsn;   
            }      
        } else if (type == CLR) {
            tx_table[tx_id].status = U;
            CompensationLogRecord * u_ptr = dynamic_cast<CompensationLogRecord *>(log_it);
            page_id = u_ptr->getPageID();
            auto p_it = dirty_page_table.find(page_id);
            if(p_it == dirty_page_table.end()){
                dirty_page_table[page_id] = lsn;   
            }  
        } else if (type == END){
            tx_table.erase(tx_id); 
        }
        ++it;
    }
}


/*
 * Run the redo phase of ARIES.
 * If the StorageEngine stops responding, return false.
 * Else when redo phase is complete, return true. 
 */

// start from the log recrod that has the smallst recLSN of all pages in the dirty page table
bool LogMgr::redo(vector <LogRecord*> log){
    int page_id = 0, offset = 0, lsn = 0;
    string after_image = "";
    //for(auto log_ptr : log){
    for(auto r_it = log.rbegin(); r_it != log.rend(); ++r_it){
        LogRecord * log_ptr = *r_it;
        lsn = log_ptr->getLSN();
        //tx_id = log_ptr->getTxID();    
        //TODO: change to switch case statement
        if(log_ptr->getType() == UPDATE){
            page_id =  dynamic_cast<UpdateLogRecord *>(log_ptr)->getPageID();
            after_image = dynamic_cast<UpdateLogRecord *>(log_ptr)->getAfterImage();
            offset = dynamic_cast<UpdateLogRecord *>(log_ptr)->getOffset();
        } else if (log_ptr->getType() == CLR){
            page_id =  dynamic_cast<CompensationLogRecord *>(log_ptr)->getPageID();
            after_image = dynamic_cast<CompensationLogRecord *>(log_ptr)->getAfterImage();
            offset = dynamic_cast<CompensationLogRecord *>(log_ptr)->getOffset();
        //}
        //else if (log_ptr->getType() == COMMIT) {
        //    logtail.push_back(new LogRecord(se->nextLSN(), lsn, tx_id, END));
        //    tx_table.erase(tx_id); 
        //    continue;
        } else {
            continue;    
        }
        auto p_it = dirty_page_table.find(page_id);
        if(p_it == dirty_page_table.end()){
            continue; 
        }
        if(p_it->second > lsn){
            continue;
        }
        if(se->getLSN(page_id) >= lsn){
            continue;
        } 
        if(!(se->pageWrite(page_id, offset, after_image, lsn))){
            return false;
        } 
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
    TxStatus status;
    LogRecord * log_rec_ptr = nullptr;
    int lsn = 0;
    string before_image = "";
    list<int> to_delete_list;

    if(txnum == NULL_TX){
        for(auto it = tx_table.rbegin(); it != tx_table.rend(); ++it){
            status = it->second.status;
            if(status == U){
                ToUndo.push(it->second.lastLSN); 
            } else {
                to_delete_list.push_back(it->first);
                logtail.push_back(new LogRecord(se->nextLSN(), getLastLSN(it->first), it->first, END));
            } 
        }
        for(auto ele : to_delete_list){
            tx_table.erase(ele);    
        }
    } else {
        ToUndo.push((*log.rbegin())->getprevLSN()); 
    }
    auto log_it = log.rbegin();
    while(!(ToUndo.empty())){
        lsn = ToUndo.top();
        ToUndo.pop();

        while(log_it != log.rend()){
            if((*log_it)->getLSN() == lsn){
                log_rec_ptr = *log_it;
                ++log_it;
                break; 
            }  
            ++log_it;
        }

        assert(log_rec_ptr != nullptr); 
        if(log_rec_ptr->getType() == UPDATE){
            UpdateLogRecord * u_ptr = dynamic_cast<UpdateLogRecord *>(log_rec_ptr);
            int lsn = se->nextLSN();
            logtail.push_back(new CompensationLogRecord(lsn, getLastLSN(u_ptr->getTxID()), u_ptr->getTxID(), 
                           u_ptr->getPageID(), u_ptr->getOffset(),u_ptr->getBeforeImage(), u_ptr->getprevLSN()));
            setLastLSN(u_ptr->getTxID(), lsn);
            tx_table[u_ptr->getTxID()].status = U;
            if(u_ptr->getprevLSN() != NULL_LSN){
                ToUndo.push(u_ptr->getprevLSN());
            } else {
               logtail.push_back(new LogRecord(se->nextLSN(), getLastLSN(u_ptr->getTxID()), u_ptr->getTxID(), END));
               tx_table.erase(u_ptr->getTxID());
            }
        } else if (log_rec_ptr->getType() == CLR){
            CompensationLogRecord * c_ptr = dynamic_cast<CompensationLogRecord *>(log_rec_ptr);

            if(c_ptr->getUndoNextLSN() != NULL_LSN){
                ToUndo.push(c_ptr->getUndoNextLSN());   
            } else {
                logtail.push_back(new LogRecord(se->nextLSN(), getLastLSN(c_ptr->getTxID()), c_ptr->getTxID(), END));
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
    int lsn = se->nextLSN();
    int last_lsn = getLastLSN(txid);
    logtail.push_back(new LogRecord(lsn, last_lsn, txid, ABORT));
    setLastLSN(txid, lsn);
    
    if(last_lsn != NULL_LSN && tx_table[txid].status != C){ 
        string log = se->getLog();    
        vector <LogRecord*> log_vec = stringToLRVector(log);
        for(auto log_ptr : logtail){
            log_vec.push_back(log_ptr);
        }
        tx_table[txid].status = U;
        undo(log_vec, txid); 
    } else if (last_lsn == NULL_LSN){
        logtail.push_back(new LogRecord(se->nextLSN(), getLastLSN(txid), txid, END));
        tx_table.erase(txid);
    } else {
        tx_table.erase(txid);
        lsn = se->nextLSN();
        logtail.push_back(new LogRecord(lsn, getLastLSN(txid), txid, END));
        logtail.push_back(new LogRecord(se->nextLSN(), lsn, txid, END));
    }
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
    //dirty_page_table.clear();
    //tx_table.clear();
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
    vector<LogRecord*> rev_log;
    int begin_check_lsn = se->get_master(); 
    auto log_it = log_vect.rbegin();
    while(log_it != log_vect.rend()){
        if((*log_it)->getType() == BEGIN_CKPT){
            break;    
        } else {
            rev_log.push_back(*log_it);    
        }
        ++log_it;
    }
    analyze(rev_log);
    int sml_lsn = INT_MAX;
    for(auto it = dirty_page_table.begin(); it != dirty_page_table.end(); ++it){
        if(it->second < sml_lsn){
            sml_lsn = it->second;    
        }
    }   
    if(begin_check_lsn != NULL_LSN && begin_check_lsn < sml_lsn){
        while(rev_log.back()->getLSN() < sml_lsn){
            rev_log.pop_back();    
        }  
    }  else {
        while(log_it != log_vect.rend()){
            if((*log_it)->getLSN() >= sml_lsn){
                rev_log.push_back(*log_it);    
            } else {
                break;    
            }
            ++log_it;    
        }
    }
    if(!redo(rev_log)){
        return;    
    }
    undo(log_vect);
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

