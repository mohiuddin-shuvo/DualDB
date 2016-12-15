// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include <sstream>
#include <fstream>

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);

  //Baseline Two
  
//  virtual Status Put(const WriteOptions& options,
//                     const Slice& value);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key, std::vector<std::string>& value_list, std::string& t, bool isQuery);

  virtual Status Get(const ReadOptions& options,
                       const Slice& keyd, const Slice& keyq, std::vector<std::string>& users,  std::vector<std::string>& events, std::string& tnow, std::string& tmin);
//
//  virtual Status SGet(const ReadOptions& options,
//                     const Slice& key, std::vector<KeyValuePair>* value_list, DB* db);

  //Continuous Query DB
  virtual Status PutC(const WriteOptions& options, const Slice& key, const Slice& json_value, std::vector<std::string>& users);

  virtual Status GetC(const ReadOptions& options,
                     const Slice& key, const Slice& value, std::vector<std::string>& events);

  virtual Status GetAllUsers(const Slice& key, std::string& tnow, std::vector<std::string>& results);
  virtual Status GetAllEvents(const ReadOptions& options,const Slice& key, std::string& tmin,  std::vector<std::string>& results);

  virtual Status GetBaseComplexQuery(const ReadOptions& options,
             const Slice& key, const Slice& value, std::vector<std::string>& users, std::vector<std::string>& events);

  virtual Status PutBaseComplexQuery(const WriteOptions& options,
               const Slice& key, const Slice& value, std::vector<std::string>& users, std::vector<std::string>& events);

  static std::string GetAttr(const rapidjson::Document& doc, const char* attr) {
    if(!doc.IsObject() || !doc.HasMember(attr) || doc[attr].IsNull())
      return "";

    std::ostringstream pKey;

    if(doc[attr].IsNumber()) {
      if(doc[attr].IsUint64()) {
        unsigned long long int tid = doc[attr].GetUint64();
        pKey<<tid;
      }
      else if (doc[attr].IsInt64()) {
        long long int tid = doc[attr].GetInt64();
        pKey<<tid;
      }
      else if (doc[attr].IsDouble()) {
        double tid = doc[attr].GetDouble();
        pKey<<tid;
      }
      else if (doc[attr].IsUint()) {
        unsigned int tid = doc[attr].GetUint();
        pKey<<tid;
      }
      else if (doc[attr].IsInt()) {
        int tid = doc[attr].GetInt();
        pKey<<tid;
      }
    }
    else if (doc[attr].IsString()) {
      const char* tid = doc[attr].GetString();
      pKey<<tid;
    }
    else if(doc[attr].IsBool()) {
      bool tid = doc[attr].GetBool();
      pKey<<tid;
    }

    return pKey.str();
  }
  static bool PushResult(std::vector<std::string>& value_list, std::string& val, std::string& t, bool isQ)
  {
	  //return false;
	  if(val.empty())
		  return true;

	  //std::cout<<isQ<<" "<<val;

	  if(isQ)
	  {

		  //return true;

		  rapidjson::Document doc;

		  doc.Parse<0>(val.data());

		  std::string uid = GetAttr(doc, "UserId");

		  if(uid.empty())
			  return true;

		  std::string tmax = GetAttr(doc, "Tmax");



		  long tmaxl = std::stol(tmax);

		  long tl = std::stol(t);

		  if(tl<=tmaxl)
			  value_list.push_back(uid);

		  //std::string tnow = GetAttr(doc, "Tnow");

//		  if(val.at(0)=='Q')
//		  {
//			  std::string qval = val.substr(1);
//			  value_list.push_back(val);
//		  }
		  //return uid;

	  }
	  else
	  {
		  rapidjson::Document doc;

		  doc.Parse<0>(val.data());

		  std::string uid = GetAttr(doc, "EventId");

		  std::string tnow = GetAttr(doc, "Tnow");

		  long tnowl = std::stol(tnow);

		  long tl = std::stol(t);

//		  if(tnowl<tl)
//			  return false;

		  if(uid.empty())
			  return true;

		  if(tnowl>=tl)
			  value_list.push_back(val);

//		  if(val.at(0)=='D')
//		  {
//			  std::string qval = val.substr(1);
//			  value_list.push_back(val);
//		  }

	  }

	  return true;

  }

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  Status CompactMemTable()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number,
                        VersionEdit* edit,
                        SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  Status BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  bool owns_info_log_;
  bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* table_cache_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;          // Signalled when background work finishes
  MemTable* mem_;
  MemTable* imm_;                // Memtable being compacted
  port::AtomicPointer has_imm_;  // So bg thread can detect non-NULL imm_
  WritableFile* logfile_;
  uint64_t logfile_number_;
  log::Writer* log_;
  uint32_t seed_;                // For sampling.

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;

  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;

  // Has a background compaction been scheduled or is running?
  bool bg_compaction_scheduled_;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;   // NULL means beginning of key range
    const InternalKey* end;     // NULL means end of key range
    InternalKey tmp_storage;    // Used to keep track of compaction progress
  };
  ManualCompaction* manual_compaction_;

  VersionSet* versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;
  int consecutive_compaction_errors_;

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;

    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) { }

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };
  CompactionStats stats_[config::kNumLevels];


  //Continuous Query DB

//  DB *datadb;
//  DB *querydb;


  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
