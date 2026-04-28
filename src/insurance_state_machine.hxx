#pragma once

#include "insurance_event.hxx"
#include "nuraft.hxx"

#include <atomic>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <string>

namespace insure_raft {

using namespace nuraft;

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

enum class PolicyStatus : int32_t { ACTIVE = 0, CANCELLED = 1 };
enum class ClaimStatus  : int32_t { FILED = 0, APPROVED = 1, DENIED = 2, PAID = 3 };

struct Policy {
    std::string  policy_id;
    std::string  holder;
    int64_t      premium_cents  = 0;
    PolicyStatus status         = PolicyStatus::ACTIVE;
    std::string  endorsements;
    int64_t      created_at_ms  = 0;
    int64_t      updated_at_ms  = 0;
};

struct Claim {
    std::string claim_id;
    std::string policy_id;
    std::string claimant;
    int64_t     amount_cents  = 0;
    int64_t     paid_cents    = 0;
    ClaimStatus status        = ClaimStatus::FILED;
    std::string notes;
    int64_t     filed_at_ms   = 0;
    int64_t     updated_at_ms = 0;
};

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

class insurance_state_machine : public state_machine {
public:
    insurance_state_machine() : last_committed_idx_(0) {}

    // ---- state_machine interface ------------------------------------------

    ptr<buffer> pre_commit(const ulong /*log_idx*/, buffer& /*data*/) override {
        return nullptr;
    }

    ptr<buffer> commit(const ulong log_idx, buffer& data) override {
        // Clone the buffer because the caller owns the original memory.
        ptr<buffer> cloned = buffer::clone(data);
        InsuranceEvent ev  = InsuranceEvent::decode(*cloned);
        apply_event(ev);
        last_committed_idx_ = log_idx;

        ptr<buffer> ret = buffer::alloc(sizeof(ulong));
        buffer_serializer bs(ret);
        bs.put_u64(log_idx);
        return ret;
    }

    void commit_config(const ulong log_idx,
                       ptr<cluster_config>& /*new_conf*/) override {
        last_committed_idx_ = log_idx;
    }

    void rollback(const ulong /*log_idx*/, buffer& /*data*/) override {}

    // ---- snapshot: read (leader sends to follower) -----------------------

    int read_logical_snp_obj(snapshot& s,
                             void*& /*user_snp_ctx*/,
                             ulong obj_id,
                             ptr<buffer>& data_out,
                             bool& is_last_obj) override
    {
        ptr<snapshot_ctx> ctx;
        {
            std::lock_guard<std::mutex> lk(snp_lock_);
            auto it = snapshots_.find(s.get_last_log_idx());
            if (it == snapshots_.end()) {
                data_out   = nullptr;
                is_last_obj = true;
                return -1;
            }
            ctx = it->second;
        }
        if (obj_id == 0) {
            // Dummy first object required by NuRaft protocol.
            data_out = buffer::alloc(sizeof(int32_t));
            buffer_serializer bs(data_out);
            bs.put_i32(0);
            is_last_obj = false;
        } else {
            data_out    = serialize_ctx(*ctx);
            is_last_obj = true;
        }
        return 0;
    }

    // ---- snapshot: write (follower receives from leader) -----------------

    void save_logical_snp_obj(snapshot& s,
                              ulong& obj_id,
                              buffer& data,
                              bool /*is_first_obj*/,
                              bool /*is_last_obj*/) override
    {
        if (obj_id == 0) {
            ptr<buffer> snp_buf = s.serialize();
            ptr<snapshot> ss    = snapshot::deserialize(*snp_buf);
            std::lock_guard<std::mutex> lk(snp_lock_);
            snapshots_[ss->get_last_log_idx()] = cs_new<snapshot_ctx>(ss);
        } else {
            std::lock_guard<std::mutex> lk(snp_lock_);
            auto it = snapshots_.find(s.get_last_log_idx());
            if (it != snapshots_.end()) {
                ptr<buffer> cloned = buffer::clone(data);
                deserialize_ctx(*cloned, *it->second);
            }
        }
        obj_id++;
    }

    bool apply_snapshot(snapshot& s) override {
        std::lock_guard<std::mutex> lk(snp_lock_);
        auto it = snapshots_.find(s.get_last_log_idx());
        if (it == snapshots_.end()) return false;

        snapshot_ctx& ctx = *it->second;
        std::lock_guard<std::mutex> dk(data_lock_);
        policies_    = ctx.policies;
        claims_      = ctx.claims;
        seen_events_ = ctx.seen_events;
        return true;
    }

    void free_user_snp_ctx(void*& /*user_snp_ctx*/) override {}

    ptr<snapshot> last_snapshot() override {
        std::lock_guard<std::mutex> lk(snp_lock_);
        if (snapshots_.empty()) return nullptr;
        return snapshots_.rbegin()->second->snapshot_;
    }

    ulong last_commit_index() override { return last_committed_idx_; }

    void create_snapshot(snapshot& s,
                         async_result<bool>::handler_type& when_done) override
    {
        ptr<buffer> snp_buf = s.serialize();
        ptr<snapshot> ss    = snapshot::deserialize(*snp_buf);

        ptr<snapshot_ctx> ctx = cs_new<snapshot_ctx>(ss);
        {
            std::lock_guard<std::mutex> dk(data_lock_);
            ctx->policies    = policies_;
            ctx->claims      = claims_;
            ctx->seen_events = seen_events_;
        }
        {
            std::lock_guard<std::mutex> lk(snp_lock_);
            snapshots_[ss->get_last_log_idx()] = ctx;
            while (snapshots_.size() > 3)
                snapshots_.erase(snapshots_.begin());
        }
        ptr<std::exception> ex(nullptr);
        bool ok = true;
        when_done(ok, ex);
    }

    // ---- query helpers ---------------------------------------------------

    void print_policies() const {
        std::lock_guard<std::mutex> lk(data_lock_);
        if (policies_.empty()) { std::cout << "  (no policies)\n"; return; }
        for (auto& kv : policies_) {
            const Policy& p = kv.second;
            std::cout << "  POLICY  " << p.policy_id
                      << "  holder: "  << p.holder
                      << "  premium: $"
                      << p.premium_cents / 100 << "."
                      << std::setw(2) << std::setfill('0')
                      << p.premium_cents % 100
                      << "  status: "
                      << (p.status == PolicyStatus::ACTIVE ? "ACTIVE" : "CANCELLED");
            if (!p.endorsements.empty())
                std::cout << "  endorsements: " << p.endorsements;
            std::cout << "\n";
        }
    }

    void print_claims() const {
        std::lock_guard<std::mutex> lk(data_lock_);
        if (claims_.empty()) { std::cout << "  (no claims)\n"; return; }
        for (auto& kv : claims_) {
            const Claim& c = kv.second;
            std::cout << "  CLAIM   " << c.claim_id
                      << "  policy: "  << c.policy_id
                      << "  claimant: " << c.claimant
                      << "  amount: $"
                      << c.amount_cents / 100 << "."
                      << std::setw(2) << std::setfill('0')
                      << c.amount_cents % 100
                      << "  status: " << claim_status_str(c.status);
            if (c.status == ClaimStatus::PAID)
                std::cout << "  paid: $" << c.paid_cents / 100;
            if (!c.notes.empty())
                std::cout << "  notes: " << c.notes;
            std::cout << "\n";
        }
    }

    size_t policy_count() const {
        std::lock_guard<std::mutex> lk(data_lock_);
        return policies_.size();
    }

    size_t claim_count() const {
        std::lock_guard<std::mutex> lk(data_lock_);
        return claims_.size();
    }

private:
    // -----------------------------------------------------------------------

    static const char* claim_status_str(ClaimStatus s) {
        switch (s) {
        case ClaimStatus::FILED:    return "FILED";
        case ClaimStatus::APPROVED: return "APPROVED";
        case ClaimStatus::DENIED:   return "DENIED";
        case ClaimStatus::PAID:     return "PAID";
        default:                    return "UNKNOWN";
        }
    }

    void apply_event(const InsuranceEvent& ev) {
        std::lock_guard<std::mutex> lk(data_lock_);

        // Idempotency: skip duplicate events.
        if (!ev.event_id.empty()) {
            if (seen_events_.count(ev.event_id)) return;
            seen_events_.insert(ev.event_id);
        }

        switch (ev.type) {
        case EventType::POLICY_CREATED: {
            Policy p;
            p.policy_id     = ev.entity_id;
            p.holder        = ev.holder;
            p.premium_cents = ev.amount_cents;
            p.status        = PolicyStatus::ACTIVE;
            p.created_at_ms = ev.timestamp_ms;
            p.updated_at_ms = ev.timestamp_ms;
            policies_[p.policy_id] = p;
            std::cout << "[SM] Policy created: " << p.policy_id
                      << " for " << p.holder << "\n";
            break;
        }
        case EventType::POLICY_UPDATED: {
            auto it = policies_.find(ev.entity_id);
            if (it == policies_.end()) break;
            if (ev.amount_cents > 0) it->second.premium_cents = ev.amount_cents;
            if (!ev.holder.empty())  it->second.holder        = ev.holder;
            it->second.updated_at_ms = ev.timestamp_ms;
            std::cout << "[SM] Policy updated: " << ev.entity_id << "\n";
            break;
        }
        case EventType::POLICY_CANCELLED: {
            auto it = policies_.find(ev.entity_id);
            if (it == policies_.end()) break;
            it->second.status       = PolicyStatus::CANCELLED;
            it->second.updated_at_ms = ev.timestamp_ms;
            std::cout << "[SM] Policy cancelled: " << ev.entity_id << "\n";
            break;
        }
        case EventType::CLAIM_FILED: {
            Claim c;
            c.claim_id      = ev.entity_id;
            c.policy_id     = ev.related_id;
            c.claimant      = ev.holder;
            c.amount_cents  = ev.amount_cents;
            c.status        = ClaimStatus::FILED;
            c.notes         = ev.notes;
            c.filed_at_ms   = ev.timestamp_ms;
            c.updated_at_ms = ev.timestamp_ms;
            claims_[c.claim_id] = c;
            std::cout << "[SM] Claim filed: " << c.claim_id
                      << " against policy " << c.policy_id << "\n";
            break;
        }
        case EventType::CLAIM_APPROVED: {
            auto it = claims_.find(ev.entity_id);
            if (it == claims_.end()) break;
            it->second.status       = ClaimStatus::APPROVED;
            it->second.updated_at_ms = ev.timestamp_ms;
            if (!ev.notes.empty()) it->second.notes = ev.notes;
            std::cout << "[SM] Claim approved: " << ev.entity_id << "\n";
            break;
        }
        case EventType::CLAIM_DENIED: {
            auto it = claims_.find(ev.entity_id);
            if (it == claims_.end()) break;
            it->second.status        = ClaimStatus::DENIED;
            it->second.updated_at_ms = ev.timestamp_ms;
            if (!ev.notes.empty()) it->second.notes = ev.notes;
            std::cout << "[SM] Claim denied: " << ev.entity_id << "\n";
            break;
        }
        case EventType::PAYMENT_ISSUED: {
            auto it = claims_.find(ev.entity_id);
            if (it == claims_.end()) break;
            it->second.paid_cents    = ev.amount_cents;
            it->second.status        = ClaimStatus::PAID;
            it->second.updated_at_ms = ev.timestamp_ms;
            std::cout << "[SM] Payment issued for " << ev.entity_id
                      << ": $" << ev.amount_cents / 100 << "\n";
            break;
        }
        case EventType::ENDORSEMENT_ADDED: {
            auto it = policies_.find(ev.entity_id);
            if (it == policies_.end()) break;
            it->second.endorsements  += "[" + ev.notes + "] ";
            it->second.updated_at_ms  = ev.timestamp_ms;
            std::cout << "[SM] Endorsement on policy " << ev.entity_id << "\n";
            break;
        }
        default:
            break;
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot serialization
    // -----------------------------------------------------------------------

    struct snapshot_ctx {
        snapshot_ctx() = default;
        explicit snapshot_ctx(ptr<snapshot> s) : snapshot_(s) {}
        ptr<snapshot> snapshot_;
        std::map<std::string, Policy> policies;
        std::map<std::string, Claim>  claims;
        std::set<std::string>         seen_events;
    };

    ptr<buffer> serialize_ctx(const snapshot_ctx& ctx) {
        // Measure required size first.
        size_t sz = sizeof(int32_t); // num_policies
        for (auto& kv : ctx.policies) {
            const Policy& p = kv.second;
            sz += (4 + p.policy_id.size()) + (4 + p.holder.size())
                + sizeof(int64_t) + sizeof(int32_t)
                + (4 + p.endorsements.size())
                + sizeof(int64_t) + sizeof(int64_t);
        }
        sz += sizeof(int32_t); // num_claims
        for (auto& kv : ctx.claims) {
            const Claim& c = kv.second;
            sz += (4 + c.claim_id.size()) + (4 + c.policy_id.size())
                + (4 + c.claimant.size())
                + sizeof(int64_t) + sizeof(int64_t) + sizeof(int32_t)
                + (4 + c.notes.size())
                + sizeof(int64_t) + sizeof(int64_t);
        }
        sz += sizeof(int32_t); // num_seen
        for (auto& s : ctx.seen_events) sz += (4 + s.size());

        ptr<buffer> buf = buffer::alloc(sz);
        buffer_serializer bs(buf);

        bs.put_i32(static_cast<int32_t>(ctx.policies.size()));
        for (auto& kv : ctx.policies) {
            const Policy& p = kv.second;
            bs.put_str(p.policy_id);
            bs.put_str(p.holder);
            bs.put_i64(p.premium_cents);
            bs.put_i32(static_cast<int32_t>(p.status));
            bs.put_str(p.endorsements);
            bs.put_i64(p.created_at_ms);
            bs.put_i64(p.updated_at_ms);
        }
        bs.put_i32(static_cast<int32_t>(ctx.claims.size()));
        for (auto& kv : ctx.claims) {
            const Claim& c = kv.second;
            bs.put_str(c.claim_id);
            bs.put_str(c.policy_id);
            bs.put_str(c.claimant);
            bs.put_i64(c.amount_cents);
            bs.put_i64(c.paid_cents);
            bs.put_i32(static_cast<int32_t>(c.status));
            bs.put_str(c.notes);
            bs.put_i64(c.filed_at_ms);
            bs.put_i64(c.updated_at_ms);
        }
        bs.put_i32(static_cast<int32_t>(ctx.seen_events.size()));
        for (auto& s : ctx.seen_events) bs.put_str(s);

        return buf;
    }

    void deserialize_ctx(buffer& data, snapshot_ctx& ctx) {
        buffer_serializer bs(data);
        ctx.policies.clear();
        ctx.claims.clear();
        ctx.seen_events.clear();

        int32_t np = bs.get_i32();
        for (int32_t i = 0; i < np; ++i) {
            Policy p;
            p.policy_id      = bs.get_str();
            p.holder         = bs.get_str();
            p.premium_cents  = bs.get_i64();
            p.status         = static_cast<PolicyStatus>(bs.get_i32());
            p.endorsements   = bs.get_str();
            p.created_at_ms  = bs.get_i64();
            p.updated_at_ms  = bs.get_i64();
            ctx.policies[p.policy_id] = p;
        }
        int32_t nc = bs.get_i32();
        for (int32_t i = 0; i < nc; ++i) {
            Claim c;
            c.claim_id      = bs.get_str();
            c.policy_id     = bs.get_str();
            c.claimant      = bs.get_str();
            c.amount_cents  = bs.get_i64();
            c.paid_cents    = bs.get_i64();
            c.status        = static_cast<ClaimStatus>(bs.get_i32());
            c.notes         = bs.get_str();
            c.filed_at_ms   = bs.get_i64();
            c.updated_at_ms = bs.get_i64();
            ctx.claims[c.claim_id] = c;
        }
        int32_t ns = bs.get_i32();
        for (int32_t i = 0; i < ns; ++i)
            ctx.seen_events.insert(bs.get_str());
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    mutable std::mutex                     data_lock_;
    std::map<std::string, Policy>          policies_;
    std::map<std::string, Claim>           claims_;
    std::set<std::string>                  seen_events_;
    std::atomic<uint64_t>                  last_committed_idx_;

    std::mutex                             snp_lock_;
    std::map<uint64_t, ptr<snapshot_ctx>>  snapshots_;
};

} // namespace insure_raft
