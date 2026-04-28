#pragma once

#include "insurance_log_store.hxx"
#include "nuraft.hxx"

#include <fstream>
#include <string>
#include <sys/stat.h>

namespace insure_raft {

using namespace nuraft;

// Persists cluster_config and srv_state to data/<data_dir>/.
// Owns the insurance_log_store for this node.
class insurance_state_mgr : public state_mgr {
public:
    insurance_state_mgr(int srv_id, const std::string& endpoint,
                        const std::string& data_dir)
        : my_id_(srv_id)
        , my_endpoint_(endpoint)
        , data_dir_(data_dir)
    {
        mkdir(data_dir_.c_str(), 0755);
        std::string log_dir = data_dir_ + "/log";
        mkdir(log_dir.c_str(), 0755);

        log_store_ = cs_new<insurance_log_store>(log_dir);

        my_srv_config_ = cs_new<srv_config>(srv_id, endpoint);

        // Load persisted cluster config or create a single-node bootstrap.
        saved_config_ = load_cluster_config();
        if (!saved_config_) {
            saved_config_ = cs_new<cluster_config>();
            saved_config_->get_servers().push_back(my_srv_config_);
        }

        saved_state_ = load_srv_state();
    }

    ptr<cluster_config> load_config() override { return saved_config_; }

    void save_config(const cluster_config& config) override {
        ptr<buffer> buf  = config.serialize();
        saved_config_    = cluster_config::deserialize(*buf);
        persist_buffer(config_path(), buf);
    }

    void save_state(const srv_state& state) override {
        ptr<buffer> buf = state.serialize();
        saved_state_    = srv_state::deserialize(*buf);
        persist_buffer(state_path(), buf);
    }

    ptr<srv_state> read_state() override { return saved_state_; }

    ptr<log_store> load_log_store() override { return log_store_; }

    int32 server_id() override { return my_id_; }

    void system_exit(const int /*exit_code*/) override {}

    ptr<srv_config> get_srv_config() const { return my_srv_config_; }

private:
    std::string config_path() const { return data_dir_ + "/cluster_config.bin"; }
    std::string state_path()  const { return data_dir_ + "/server_state.bin"; }

    static void persist_buffer(const std::string& path, const ptr<buffer>& buf) {
        std::ofstream f(path, std::ios::binary | std::ios::trunc);
        if (!f) return;
        uint32_t sz = static_cast<uint32_t>(buf->size());
        f.write(reinterpret_cast<const char*>(&sz), sizeof(sz));
        f.write(reinterpret_cast<const char*>(buf->data_begin()), sz);
    }

    static ptr<buffer> read_buffer(const std::string& path) {
        std::ifstream f(path, std::ios::binary);
        if (!f) return nullptr;
        uint32_t sz = 0;
        f.read(reinterpret_cast<char*>(&sz), sizeof(sz));
        if (!f || sz == 0) return nullptr;
        ptr<buffer> buf = buffer::alloc(sz);
        f.read(reinterpret_cast<char*>(buf->data_begin()), sz);
        return f ? buf : nullptr;
    }

    ptr<cluster_config> load_cluster_config() {
        ptr<buffer> buf = read_buffer(config_path());
        if (!buf) return nullptr;
        return cluster_config::deserialize(*buf);
    }

    ptr<srv_state> load_srv_state() {
        ptr<buffer> buf = read_buffer(state_path());
        if (!buf) return nullptr;
        return srv_state::deserialize(*buf);
    }

    int                         my_id_;
    std::string                 my_endpoint_;
    std::string                 data_dir_;
    ptr<insurance_log_store>    log_store_;
    ptr<srv_config>             my_srv_config_;
    ptr<cluster_config>         saved_config_;
    ptr<srv_state>              saved_state_;
};

} // namespace insure_raft
