#include "insurance_log_store.hxx"

#include "nuraft.hxx"

#include <cassert>
#include <cstdio>
#include <dirent.h>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <sys/stat.h>

namespace insure_raft {

using namespace nuraft;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

static void ensure_dir(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

static ptr<log_entry> dummy_entry() {
    ptr<buffer> buf = buffer::alloc(sizeof(ulong));
    return cs_new<log_entry>(0, buf);
}

insurance_log_store::insurance_log_store(const std::string& log_dir)
    : log_dir_(log_dir)
    , start_idx_(1)
{
    ensure_dir(log_dir_);
    // Dummy entry at slot 0 (NuRaft convention).
    logs_[0] = dummy_entry();
    load_all();
}

insurance_log_store::~insurance_log_store() {}

// ---------------------------------------------------------------------------
// path helpers
// ---------------------------------------------------------------------------

std::string insurance_log_store::entry_path(ulong index) const {
    std::ostringstream oss;
    oss << log_dir_ << "/entry_"
        << std::setw(8) << std::setfill('0') << index << ".bin";
    return oss.str();
}

// ---------------------------------------------------------------------------
// persistence
// ---------------------------------------------------------------------------

void insurance_log_store::persist_entry(ulong index, const ptr<log_entry>& e) {
    ptr<buffer> buf = e->serialize();
    std::ofstream f(entry_path(index), std::ios::binary | std::ios::trunc);
    if (f) {
        uint32_t sz = static_cast<uint32_t>(buf->size());
        f.write(reinterpret_cast<const char*>(&sz), sizeof(sz));
        f.write(reinterpret_cast<const char*>(buf->data_begin()), sz);
    }
}

ptr<log_entry> insurance_log_store::load_entry(ulong index) const {
    std::ifstream f(entry_path(index), std::ios::binary);
    if (!f) return nullptr;
    uint32_t sz = 0;
    f.read(reinterpret_cast<char*>(&sz), sizeof(sz));
    if (!f || sz == 0) return nullptr;
    ptr<buffer> buf = buffer::alloc(sz);
    f.read(reinterpret_cast<char*>(buf->data_begin()), sz);
    if (!f) return nullptr;
    return log_entry::deserialize(*buf);
}

void insurance_log_store::delete_entry_file(ulong index) {
    std::remove(entry_path(index).c_str());
}

void insurance_log_store::save_start_idx() {
    std::string path = log_dir_ + "/start_idx.bin";
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (f) {
        ulong v = start_idx_.load();
        f.write(reinterpret_cast<const char*>(&v), sizeof(v));
    }
}

void insurance_log_store::load_all() {
    // Load persisted start_idx if present.
    {
        std::string path = log_dir_ + "/start_idx.bin";
        std::ifstream f(path, std::ios::binary);
        if (f) {
            ulong v = 0;
            f.read(reinterpret_cast<char*>(&v), sizeof(v));
            if (v >= 1) start_idx_ = v;
        }
    }

    // Scan directory for entry_XXXXXXXX.bin files.
    DIR* d = opendir(log_dir_.c_str());
    if (!d) return;

    std::vector<ulong> indices;
    struct dirent* ent;
    while ((ent = readdir(d)) != nullptr) {
        std::string name(ent->d_name);
        if (name.size() < 14) continue; // "entry_XXXXXXXX.bin" = 18 chars
        if (name.substr(0, 6) != "entry_") continue;
        if (name.substr(name.size() - 4) != ".bin") continue;
        std::string idx_str = name.substr(6, name.size() - 10);
        try {
            ulong idx = std::stoul(idx_str);
            indices.push_back(idx);
        } catch (...) {}
    }
    closedir(d);

    std::sort(indices.begin(), indices.end());
    for (ulong idx : indices) {
        if (idx < start_idx_) {
            // Stale file from before a compact — remove it.
            delete_entry_file(idx);
            continue;
        }
        ptr<log_entry> e = load_entry(idx);
        if (e) logs_[idx] = e;
    }
}

// ---------------------------------------------------------------------------
// clone helper
// ---------------------------------------------------------------------------

ptr<log_entry> insurance_log_store::clone_entry(const ptr<log_entry>& e) {
    return cs_new<log_entry>(
        e->get_term(),
        buffer::clone(e->get_buf()),
        e->get_val_type());
}

// ---------------------------------------------------------------------------
// log_store interface
// ---------------------------------------------------------------------------

ulong insurance_log_store::next_slot() const {
    std::lock_guard<std::mutex> lk(lock_);
    return start_idx_ + logs_.size() - 1;
}

ulong insurance_log_store::start_index() const {
    return start_idx_;
}

ptr<log_entry> insurance_log_store::last_entry() const {
    ulong next = next_slot();
    std::lock_guard<std::mutex> lk(lock_);
    auto it = logs_.find(next - 1);
    if (it == logs_.end()) it = logs_.find(0);
    return clone_entry(it->second);
}

ulong insurance_log_store::append(ptr<log_entry>& entry) {
    ptr<log_entry> clone = clone_entry(entry);
    std::lock_guard<std::mutex> lk(lock_);
    ulong idx = start_idx_ + logs_.size() - 1;
    logs_[idx] = clone;
    persist_entry(idx, clone);
    return idx;
}

void insurance_log_store::write_at(ulong index, ptr<log_entry>& entry) {
    ptr<log_entry> clone = clone_entry(entry);
    std::lock_guard<std::mutex> lk(lock_);
    // Truncate all entries >= index.
    auto it = logs_.lower_bound(index);
    while (it != logs_.end()) {
        delete_entry_file(it->first);
        it = logs_.erase(it);
    }
    logs_[index] = clone;
    persist_entry(index, clone);
}

ptr<std::vector<ptr<log_entry>>>
insurance_log_store::log_entries(ulong start, ulong end) {
    auto ret = cs_new<std::vector<ptr<log_entry>>>();
    ret->resize(end - start);
    ulong cc = 0;
    for (ulong ii = start; ii < end; ++ii) {
        ptr<log_entry> src;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = logs_.find(ii);
            src = (it != logs_.end()) ? it->second : logs_.find(0)->second;
        }
        (*ret)[cc++] = clone_entry(src);
    }
    return ret;
}

ptr<std::vector<ptr<log_entry>>>
insurance_log_store::log_entries_ext(ulong start, ulong end,
                                     int64 batch_size_hint_in_bytes) {
    auto ret = cs_new<std::vector<ptr<log_entry>>>();
    if (batch_size_hint_in_bytes < 0) return ret;

    size_t accum = 0;
    for (ulong ii = start; ii < end; ++ii) {
        ptr<log_entry> src;
        {
            std::lock_guard<std::mutex> lk(lock_);
            auto it = logs_.find(ii);
            src = (it != logs_.end()) ? it->second : logs_.find(0)->second;
        }
        ret->push_back(clone_entry(src));
        accum += src->get_buf().size();
        if (batch_size_hint_in_bytes &&
            accum >= static_cast<size_t>(batch_size_hint_in_bytes)) break;
    }
    return ret;
}

ptr<log_entry> insurance_log_store::entry_at(ulong index) {
    std::lock_guard<std::mutex> lk(lock_);
    auto it = logs_.find(index);
    if (it == logs_.end()) it = logs_.find(0);
    return clone_entry(it->second);
}

ulong insurance_log_store::term_at(ulong index) {
    std::lock_guard<std::mutex> lk(lock_);
    auto it = logs_.find(index);
    if (it == logs_.end()) it = logs_.find(0);
    return it->second->get_term();
}

ptr<buffer> insurance_log_store::pack(ulong index, int32 cnt) {
    std::vector<ptr<buffer>> serialized;
    size_t total = 0;
    for (ulong ii = index; ii < index + cnt; ++ii) {
        ptr<log_entry> e;
        {
            std::lock_guard<std::mutex> lk(lock_);
            e = logs_[ii];
        }
        ptr<buffer> buf = e->serialize();
        total += buf->size();
        serialized.push_back(buf);
    }

    ptr<buffer> out = buffer::alloc(sizeof(int32) + cnt * sizeof(int32) + total);
    out->pos(0);
    out->put(static_cast<int32>(cnt));
    for (auto& b : serialized) {
        out->put(static_cast<int32>(b->size()));
        out->put(*b);
    }
    return out;
}

void insurance_log_store::apply_pack(ulong index, buffer& pack) {
    pack.pos(0);
    int32 num = pack.get_int();
    for (int32 ii = 0; ii < num; ++ii) {
        ulong cur = index + ii;
        int32 sz  = pack.get_int();
        ptr<buffer> buf = buffer::alloc(sz);
        pack.get(buf);
        ptr<log_entry> e = log_entry::deserialize(*buf);
        {
            std::lock_guard<std::mutex> lk(lock_);
            logs_[cur] = e;
        }
        persist_entry(cur, e);
    }
    {
        std::lock_guard<std::mutex> lk(lock_);
        auto it = logs_.upper_bound(0);
        start_idx_ = (it != logs_.end()) ? it->first : 1;
    }
    save_start_idx();
}

bool insurance_log_store::compact(ulong last_log_index) {
    std::lock_guard<std::mutex> lk(lock_);
    for (ulong ii = start_idx_; ii <= last_log_index; ++ii) {
        auto it = logs_.find(ii);
        if (it != logs_.end()) {
            delete_entry_file(ii);
            logs_.erase(it);
        }
    }
    if (start_idx_ <= last_log_index) {
        start_idx_ = last_log_index + 1;
    }
    save_start_idx();
    return true;
}

bool insurance_log_store::flush() {
    return true;
}

} // namespace insure_raft
