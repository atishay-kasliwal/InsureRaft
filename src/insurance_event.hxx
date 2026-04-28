#pragma once

#include "nuraft.hxx"

#include <chrono>
#include <string>

namespace insure_raft {

using namespace nuraft;

enum class EventType : uint32_t {
    POLICY_CREATED    = 1,
    POLICY_UPDATED    = 2,
    POLICY_CANCELLED  = 3,
    CLAIM_FILED       = 4,
    CLAIM_APPROVED    = 5,
    CLAIM_DENIED      = 6,
    PAYMENT_ISSUED    = 7,
    ENDORSEMENT_ADDED = 8,
};

inline const char* event_type_str(EventType t) {
    switch (t) {
    case EventType::POLICY_CREATED:    return "POLICY_CREATED";
    case EventType::POLICY_UPDATED:    return "POLICY_UPDATED";
    case EventType::POLICY_CANCELLED:  return "POLICY_CANCELLED";
    case EventType::CLAIM_FILED:       return "CLAIM_FILED";
    case EventType::CLAIM_APPROVED:    return "CLAIM_APPROVED";
    case EventType::CLAIM_DENIED:      return "CLAIM_DENIED";
    case EventType::PAYMENT_ISSUED:    return "PAYMENT_ISSUED";
    case EventType::ENDORSEMENT_ADDED: return "ENDORSEMENT_ADDED";
    default:                           return "UNKNOWN";
    }
}

struct InsuranceEvent {
    EventType   type;
    std::string event_id;      // globally unique idempotency key
    std::string entity_id;     // policy_id or claim_id
    std::string related_id;    // policy_id when filing a claim, else empty
    std::string holder;        // policyholder name / claimant
    int64_t     amount_cents;  // premium or claim/payment amount in cents
    std::string notes;
    int64_t     timestamp_ms;

    // Wire format: put_u32(type) + put_str x5 + put_i64 x2
    static ptr<buffer> encode(const InsuranceEvent& ev) {
        size_t sz = sizeof(uint32_t)
                  + (4 + ev.event_id.size())
                  + (4 + ev.entity_id.size())
                  + (4 + ev.related_id.size())
                  + (4 + ev.holder.size())
                  + sizeof(int64_t)
                  + (4 + ev.notes.size())
                  + sizeof(int64_t);

        ptr<buffer> buf = buffer::alloc(sz);
        buffer_serializer bs(buf);
        bs.put_u32(static_cast<uint32_t>(ev.type));
        bs.put_str(ev.event_id);
        bs.put_str(ev.entity_id);
        bs.put_str(ev.related_id);
        bs.put_str(ev.holder);
        bs.put_i64(ev.amount_cents);
        bs.put_str(ev.notes);
        bs.put_i64(ev.timestamp_ms);
        return buf;
    }

    static InsuranceEvent decode(buffer& buf) {
        InsuranceEvent ev;
        buffer_serializer bs(buf);
        ev.type         = static_cast<EventType>(bs.get_u32());
        ev.event_id     = bs.get_str();
        ev.entity_id    = bs.get_str();
        ev.related_id   = bs.get_str();
        ev.holder       = bs.get_str();
        ev.amount_cents = bs.get_i64();
        ev.notes        = bs.get_str();
        ev.timestamp_ms = bs.get_i64();
        return ev;
    }

    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    // Build a unique event ID from type + entity + timestamp.
    static std::string make_id(EventType t, const std::string& entity_id) {
        return std::to_string(static_cast<uint32_t>(t))
             + "_" + entity_id
             + "_" + std::to_string(now_ms());
    }
};

} // namespace insure_raft
