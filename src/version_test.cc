#include "util/filename.h"
#include "test_util/testharness.h"

#include "edit_collector.h"
#include "testutil.h"
#include "util.h"
#include "version_edit.h"
#include "version_set.h"

namespace rocksdb {
namespace titandb {

void DeleteDir(Env* env, const std::string& dirname) {
  std::vector<std::string> filenames;
  env->GetChildren(dirname, &filenames);
  for (auto& fname : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(fname, &number, &type)) {
      ASSERT_OK(env->DeleteFile(dirname + "/" + fname));
    }
  }
  ASSERT_OK(env->DeleteDir(dirname));
}

class VersionTest : public testing::Test {
 public:
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  std::shared_ptr<BlobFileCache> file_cache_;
  std::map<uint32_t, std::shared_ptr<BlobStorage>> column_families_;
  std::unique_ptr<VersionSet> vset_;
  port::Mutex mutex_;
  std::string dbname_;
  Env* env_;

  VersionTest() : dbname_(test::TmpDir()), env_(Env::Default()) {
    db_options_.dirname = dbname_ + "/titandb";
    db_options_.create_if_missing = true;
    env_->CreateDirIfMissing(dbname_);
    env_->CreateDirIfMissing(db_options_.dirname);
    auto cache = NewLRUCache(db_options_.max_open_files);
    file_cache_.reset(
        new BlobFileCache(db_options_, cf_options_, cache, nullptr));
    Reset();
  }

  void Reset() {
    DeleteDir(env_, db_options_.dirname);
    env_->CreateDirIfMissing(db_options_.dirname);

    vset_.reset(new VersionSet(db_options_, nullptr));
    ASSERT_OK(vset_->Open({}));
    column_families_.clear();
    // Sets up some column families.
    for (uint32_t id = 0; id < 10; id++) {
      std::shared_ptr<BlobStorage> storage;
      storage.reset(
          new BlobStorage(db_options_, cf_options_, id, file_cache_, nullptr));
      column_families_.emplace(id, storage);
      storage.reset(
          new BlobStorage(db_options_, cf_options_, id, file_cache_, nullptr));
      vset_->column_families_.emplace(id, storage);
    }
  }

  void AddBlobFiles(uint32_t cf_id, uint64_t start, uint64_t end) {
    auto storage = column_families_[cf_id];
    for (auto i = start; i < end; i++) {
      auto file = std::make_shared<BlobFileMeta>(i, i);
      storage->files_.emplace(i, file);
    }
  }

  void DeleteBlobFiles(uint32_t cf_id, uint64_t start, uint64_t end) {
    auto& storage = column_families_[cf_id];
    for (auto i = start; i < end; i++) {
      storage->files_.erase(i);
    }
  }

  void BuildAndCheck(std::vector<VersionEdit> edits) {
    EditCollector collector;
    for (auto& edit : edits) {
      ASSERT_OK(collector.AddEdit(edit));
    }
    ASSERT_OK(collector.Seal(*vset_.get()));
    ASSERT_OK(collector.Apply(*vset_.get()));
    for (auto& it : vset_->column_families_) {
      auto& storage = column_families_[it.first];
      // ignore obsolete file
      auto size = 0;
      for (auto& file : it.second->files_) {
        if (!file.second->is_obsolete()) {
          size++;
        }
      }
      ASSERT_EQ(storage->files_.size(), size);
      for (auto& f : storage->files_) {
        auto iter = it.second->files_.find(f.first);
        ASSERT_TRUE(iter != it.second->files_.end());
        ASSERT_EQ(*f.second, *(iter->second));
      }
    }
  }

  void CheckColumnFamiliesSize(uint64_t size) {
    ASSERT_EQ(vset_->column_families_.size(), size);
  }
};

TEST_F(VersionTest, VersionEdit) {
  VersionEdit input;
  CheckCodec(input);
  input.SetNextFileNumber(1);
  input.SetColumnFamilyID(2);
  CheckCodec(input);
  auto file1 = std::make_shared<BlobFileMeta>(3, 4);
  auto file2 = std::make_shared<BlobFileMeta>(5, 6);
  input.AddBlobFile(file1);
  input.AddBlobFile(file2);
  input.DeleteBlobFile(7);
  input.DeleteBlobFile(8);
  CheckCodec(input);
}

VersionEdit AddBlobFilesEdit(uint32_t cf_id, uint64_t start, uint64_t end) {
  VersionEdit edit;
  edit.SetColumnFamilyID(cf_id);
  for (auto i = start; i < end; i++) {
    auto file = std::make_shared<BlobFileMeta>(i, i);
    edit.AddBlobFile(file);
  }
  return edit;
}

VersionEdit DeleteBlobFilesEdit(uint32_t cf_id, uint64_t start, uint64_t end) {
  VersionEdit edit;
  edit.SetColumnFamilyID(cf_id);
  for (auto i = start; i < end; i++) {
    edit.DeleteBlobFile(i);
  }
  return edit;
}

TEST_F(VersionTest, InvalidEdit) {
  // init state
  {
    auto add1_0_4 = AddBlobFilesEdit(1, 0, 4);
    EditCollector collector;
    ASSERT_OK(collector.AddEdit(add1_0_4));
    ASSERT_OK(collector.Seal(*vset_.get()));
    ASSERT_OK(collector.Apply(*vset_.get()));
  }

  // delete nonexistent blobs
  {
    auto del1_4_6 = DeleteBlobFilesEdit(1, 4, 6);
    EditCollector collector;
    ASSERT_OK(collector.AddEdit(del1_4_6));
    ASSERT_NOK(collector.Seal(*vset_.get()));
    ASSERT_NOK(collector.Apply(*vset_.get()));
  }

  // add already existing blobs
  {
    auto add1_1_3 = AddBlobFilesEdit(1, 1, 3);
    EditCollector collector;
    ASSERT_OK(collector.AddEdit(add1_1_3));
    ASSERT_NOK(collector.Seal(*vset_.get()));
    ASSERT_NOK(collector.Apply(*vset_.get()));
  }

  // add same blobs
  {
    auto add1_4_5_1 = AddBlobFilesEdit(1, 4, 5);
    auto add1_4_5_2 = AddBlobFilesEdit(1, 4, 5);
    EditCollector collector;
    ASSERT_OK(collector.AddEdit(add1_4_5_1));
    ASSERT_NOK(collector.AddEdit(add1_4_5_2));
    ASSERT_NOK(collector.Seal(*vset_.get()));
    ASSERT_NOK(collector.Apply(*vset_.get()));
  }

  // delete same blobs
  {
    auto del1_3_4_1 = DeleteBlobFilesEdit(1, 3, 4);
    auto del1_3_4_2 = DeleteBlobFilesEdit(1, 3, 4);
    EditCollector collector;
    ASSERT_OK(collector.AddEdit(del1_3_4_1));
    ASSERT_NOK(collector.AddEdit(del1_3_4_2));
    ASSERT_NOK(collector.Seal(*vset_.get()));
    ASSERT_NOK(collector.Apply(*vset_.get()));
  }
}

TEST_F(VersionTest, VersionBuilder) {
  // {(0, 4)}, {}
  auto add1_0_4 = AddBlobFilesEdit(1, 0, 4);
  AddBlobFiles(1, 0, 4);
  BuildAndCheck({add1_0_4});

  // {(0, 8)}, {(4, 8)}
  auto add1_4_8 = AddBlobFilesEdit(1, 4, 8);
  auto add2_4_8 = AddBlobFilesEdit(2, 4, 8);
  AddBlobFiles(1, 4, 8);
  AddBlobFiles(2, 4, 8);
  BuildAndCheck({add1_4_8, add2_4_8});

  // {(0, 4), (6, 8)}, {(4, 8)}
  auto del1_4_6 = DeleteBlobFilesEdit(1, 4, 6);
  DeleteBlobFiles(1, 4, 6);
  BuildAndCheck({del1_4_6});

  // {(0, 4)}, {(4, 6)}
  auto del1_6_8 = DeleteBlobFilesEdit(1, 6, 8);
  auto del2_6_8 = DeleteBlobFilesEdit(2, 6, 8);
  DeleteBlobFiles(1, 6, 8);
  DeleteBlobFiles(2, 6, 8);
  BuildAndCheck({del1_6_8, del2_6_8});

  // {(0, 4)}, {(4, 6)}
  Reset();
  AddBlobFiles(1, 0, 4);
  AddBlobFiles(2, 4, 6);
  add1_0_4 = AddBlobFilesEdit(1, 0, 4);
  add1_4_8 = AddBlobFilesEdit(1, 4, 8);
  add2_4_8 = AddBlobFilesEdit(2, 4, 8);
  del1_4_6 = DeleteBlobFilesEdit(1, 4, 6);
  del1_6_8 = DeleteBlobFilesEdit(1, 6, 8);
  del2_6_8 = DeleteBlobFilesEdit(2, 6, 8);
  BuildAndCheck({add1_0_4, add1_4_8, del1_4_6, del1_6_8, add2_4_8, del2_6_8});
}

TEST_F(VersionTest, ObsoleteFiles) {
  CheckColumnFamiliesSize(10);
  std::map<uint32_t, TitanCFOptions> m;
  m.insert({1, TitanCFOptions()});
  m.insert({2, TitanCFOptions()});
  vset_->AddColumnFamilies(m);
  {
    auto add1_1_5 = AddBlobFilesEdit(1, 1, 5);
    MutexLock l(&mutex_);
    vset_->LogAndApply(add1_1_5);
  }
  std::vector<std::string> of;
  vset_->GetObsoleteFiles(&of, kMaxSequenceNumber);
  ASSERT_EQ(of.size(), 0);
  {
    auto del1_4_5 = DeleteBlobFilesEdit(1, 4, 5);
    MutexLock l(&mutex_);
    vset_->LogAndApply(del1_4_5);
  }
  vset_->GetObsoleteFiles(&of, kMaxSequenceNumber);
  ASSERT_EQ(of.size(), 1);

  std::vector<uint32_t> cfs = {1};
  ASSERT_OK(vset_->DropColumnFamilies(cfs, 0));
  vset_->GetObsoleteFiles(&of, kMaxSequenceNumber);
  ASSERT_EQ(of.size(), 1);
  CheckColumnFamiliesSize(10);

  ASSERT_OK(vset_->MaybeDestroyColumnFamily(1));
  vset_->GetObsoleteFiles(&of, kMaxSequenceNumber);
  ASSERT_EQ(of.size(), 4);
  CheckColumnFamiliesSize(9);

  ASSERT_OK(vset_->MaybeDestroyColumnFamily(2));
  CheckColumnFamiliesSize(8);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
