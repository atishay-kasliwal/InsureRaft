#pragma once

#include "log_store.hxx"
#include "nuraft.hxx"

#include <atomic>
#include <map>
#include <mutex>
#include <string>

namespace insure_raft {

using namespace nuraft;

// File-backed log store.
// Each entry is persisted as data/<dir>/entry_XXXXXXXX.bin.
// On startup the directory is scanned and entries loaded into memory.
// write_at truncates forward; compact removes trailing-start files.
class insurance_log_store : public log_store {
public:
    explicit insurance_log_store(const std::string& log_dir);
    ~insurance_log_store();

    ulong next_slot() const override;
    ulong start_index() const override;
    ptr<log_entry> last_entry() const override;
    ulong append(ptr<log_entry>& entry) override;
    void write_at(ulong index, ptr<log_entry>& entry) override;
    ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) override;
    ptr<std::vector<ptr<log_entry>>> log_entries_ext(
        ulong start, ulong end, int64 batch_size_hint_in_bytes = 0) override;
    ptr<log_entry> entry_at(ulong index) override;
    ulong term_at(ulong index) override;
    ptr<buffer> pack(ulong index, int32 cnt) override;
    void apply_pack(ulong index, buffer& pack) override;
    bool compact(ulong last_log_index) override;
    bool flush() override;

private:
    static ptr<log_entry> clone_entry(const ptr<log_entry>& e);
    std::string entry_path(ulong index) const;
    void persist_entry(ulong index, const ptr<log_entry>& e);
    ptr<log_entry> load_entry(ulong index) const;
    void delete_entry_file(ulong index);
    void save_start_idx();
    void load_all();

    std::string log_dir_;

    mutable std::mutex lock_;
    std::map<ulong, ptr<log_entry>> logs_;
    std::atomic<ulong> start_idx_;
};

} // namespace insure_raft
