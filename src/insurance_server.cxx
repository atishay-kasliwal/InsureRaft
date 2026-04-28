#include "insurance_state_machine.hxx"
#include "insurance_state_mgr.hxx"
#include "logger_wrapper.hxx"

#include "nuraft.hxx"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace nuraft;
using namespace insure_raft;

// ---------------------------------------------------------------------------
// Global server state
// ---------------------------------------------------------------------------

struct ServerStuff {
    int         server_id_  = 1;
    std::string addr_       = "localhost";
    int         port_       = 26000;
    std::string endpoint_;
    std::string data_dir_;

    ptr<logger>                  raft_logger_;
    ptr<state_machine>           sm_;
    ptr<state_mgr>               smgr_;
    raft_launcher                launcher_;
    ptr<raft_server>             raft_instance_;

    void reset() {
        raft_logger_.reset();
        sm_.reset();
        smgr_.reset();
        raft_instance_.reset();
    }
};

static ServerStuff g;

using raft_result = cmd_result<ptr<buffer>>;

// ---------------------------------------------------------------------------
// Helper: tokenize
// ---------------------------------------------------------------------------

static std::vector<std::string> tokenize(const std::string& line) {
    std::vector<std::string> tokens;
    std::istringstream iss(line);
    std::string tok;
    while (iss >> tok) tokens.push_back(tok);
    return tokens;
}

// ---------------------------------------------------------------------------
// Helper: submit an InsuranceEvent to Raft and wait for consensus.
// ---------------------------------------------------------------------------

static bool submit(const InsuranceEvent& ev) {
    ptr<buffer> log = InsuranceEvent::encode(ev);
    ptr<raft_result> ret = g.raft_instance_->append_entries({log});

    if (!ret->get_accepted()) {
        std::cout << "  rejected (not leader or cluster unavailable): "
                  << ret->get_result_code() << "\n";
        return false;
    }
    ptr<std::exception> err(nullptr);
    raft_result& r = *ret;
    if (r.get_result_code() != cmd_result_code::OK) {
        std::cout << "  commit failed: " << r.get_result_code() << "\n";
        return false;
    }
    ptr<buffer> result = r.get();
    if (result) {
        buffer_serializer bs(result);
        uint64_t log_idx = bs.get_u64();
        std::cout << "  committed at log index " << log_idx << "\n";
    }
    return true;
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

static insurance_state_machine* get_sm() {
    return static_cast<insurance_state_machine*>(g.sm_.get());
}

// policy <policy_id> <holder_name> <annual_premium_dollars>
static void cmd_policy(const std::vector<std::string>& t) {
    if (t.size() < 4) {
        std::cout << "  usage: policy <policy_id> <holder> <premium_dollars>\n";
        return;
    }
    InsuranceEvent ev;
    ev.type         = EventType::POLICY_CREATED;
    ev.entity_id    = t[1];
    ev.holder       = t[2];
    ev.amount_cents = static_cast<int64_t>(std::stod(t[3]) * 100);
    ev.timestamp_ms = InsuranceEvent::now_ms();
    ev.event_id     = InsuranceEvent::make_id(ev.type, ev.entity_id);
    submit(ev);
}

// update_policy <policy_id> <new_premium_dollars>
static void cmd_update_policy(const std::vector<std::string>& t) {
    if (t.size() < 3) {
        std::cout << "  usage: update_policy <policy_id> <new_premium_dollars>\n";
        return;
    }
    InsuranceEvent ev;
    ev.type         = EventType::POLICY_UPDATED;
    ev.entity_id    = t[1];
    ev.amount_cents = static_cast<int64_t>(std::stod(t[2]) * 100);
    ev.timestamp_ms = InsuranceEvent::now_ms();
    ev.event_id     = InsuranceEvent::make_id(ev.type, ev.entity_id);
    submit(ev);
}

// cancel_policy <policy_id>
static void cmd_cancel_policy(const std::vector<std::string>& t) {
    if (t.size() < 2) {
        std::cout << "  usage: cancel_policy <policy_id>\n";
        return;
    }
    InsuranceEvent ev;
    ev.type         = EventType::POLICY_CANCELLED;
    ev.entity_id    = t[1];
    ev.timestamp_ms = InsuranceEvent::now_ms();
    ev.event_id     = InsuranceEvent::make_id(ev.type, ev.entity_id);
    submit(ev);
}

// claim <claim_id> <policy_id> <claimant> <amount_dollars> [notes...]
static void cmd_claim(const std::vector<std::string>& t) {
    if (t.size() < 5) {
        std::cout << "  usage: claim <claim_id> <policy_id> <claimant> "
                     "<amount_dollars> [notes...]\n";
        return;
    }
    InsuranceEvent ev;
    ev.type         = EventType::CLAIM_FILED;
    ev.entity_id    = t[1];
    ev.related_id   = t[2];
    ev.holder       = t[3];
    ev.amount_cents = static_cast<int64_t>(std::stod(t[4]) * 100);
    for (size_t i = 5; i < t.size(); ++i) {
        if (i > 5) ev.notes += " ";
        ev.notes += t[i];
    }
    ev.timestamp_ms = InsuranceEvent::now_ms();
    ev.event_id     = InsuranceEvent::make_id(ev.type, ev.entity_id);
    submit(ev);
}

// approve <claim_id> [notes...]
static void cmd_approve(const std::vector<std::string>& t) {
    if (t.size() < 2) {
        std::cout << "  usage: approve <claim_id> [notes...]\n";
        return;
    }
    InsuranceEvent ev;
    ev.type         = EventType::CLAIM_APPROVED;
    ev.entity_id    = t[1];
    for (size_t i = 2; i < t.size(); ++i) {
        if (i > 2) ev.notes += " ";
        ev.notes += t[i];
    }
    ev.timestamp_ms = InsuranceEvent::now_ms();
    ev.event_id     = InsuranceEvent::make_id(ev.type, ev.entity_id);
    submit(ev);
}

// deny <claim_id> [notes...]
static void cmd_deny(const std::vector<std::string>& t) {
    if (t.size() < 2) {
        std::cout << "  usage: deny <claim_id> [notes...]\n";
        return;
    }
    InsuranceEvent ev;
    ev.type         = EventType::CLAIM_DENIED;
    ev.entity_id    = t[1];
    for (size_t i = 2; i < t.size(); ++i) {
        if (i > 2) ev.notes += " ";
        ev.notes += t[i];
    }
    ev.timestamp_ms = InsuranceEvent::now_ms();
    ev.event_id     = InsuranceEvent::make_id(ev.type, ev.entity_id);
    submit(ev);
}

// pay <claim_id> <amount_dollars>
static void cmd_pay(const std::vector<std::string>& t) {
    if (t.size() < 3) {
        std::cout << "  usage: pay <claim_id> <amount_dollars>\n";
        return;
    }
    InsuranceEvent ev;
    ev.type         = EventType::PAYMENT_ISSUED;
    ev.entity_id    = t[1];
    ev.amount_cents = static_cast<int64_t>(std::stod(t[2]) * 100);
    ev.timestamp_ms = InsuranceEvent::now_ms();
    ev.event_id     = InsuranceEvent::make_id(ev.type, ev.entity_id);
    submit(ev);
}

// endorse <policy_id> <notes...>
static void cmd_endorse(const std::vector<std::string>& t) {
    if (t.size() < 3) {
        std::cout << "  usage: endorse <policy_id> <endorsement_text...>\n";
        return;
    }
    InsuranceEvent ev;
    ev.type         = EventType::ENDORSEMENT_ADDED;
    ev.entity_id    = t[1];
    for (size_t i = 2; i < t.size(); ++i) {
        if (i > 2) ev.notes += " ";
        ev.notes += t[i];
    }
    ev.timestamp_ms = InsuranceEvent::now_ms();
    ev.event_id     = InsuranceEvent::make_id(ev.type, ev.entity_id);
    submit(ev);
}

// add <server_id> <host:port>
static void cmd_add(const std::vector<std::string>& t) {
    if (t.size() < 3) {
        std::cout << "  usage: add <server_id> <host:port>\n";
        return;
    }
    int peer_id = std::stoi(t[1]);
    if (!peer_id || peer_id == g.server_id_) {
        std::cout << "  invalid server id\n";
        return;
    }
    srv_config conf(peer_id, t[2]);
    ptr<raft_result> ret = g.raft_instance_->add_srv(conf);
    if (!ret->get_accepted())
        std::cout << "  failed: " << ret->get_result_code() << "\n";
    else
        std::cout << "  request in progress (check with 'ls')\n";
}

static void cmd_stat() {
    ptr<log_store> ls = g.smgr_->load_log_store();
    std::vector<ptr<srv_config>> cfgs;
    g.raft_instance_->get_srv_config_all(cfgs);
    int leader = g.raft_instance_->get_leader();

    std::cout << "  server id      : " << g.server_id_ << "\n"
              << "  endpoint       : " << g.endpoint_  << "\n"
              << "  leader id      : " << leader << (leader == g.server_id_ ? " (me)" : "") << "\n"
              << "  Raft log range : ";
    if (ls->start_index() >= ls->next_slot())
        std::cout << "(empty)\n";
    else
        std::cout << ls->start_index() << " - " << (ls->next_slot() - 1) << "\n";
    std::cout << "  committed idx  : "
              << g.raft_instance_->get_committed_log_idx() << "\n"
              << "  term           : "
              << g.raft_instance_->get_term() << "\n"
              << "  policies       : " << get_sm()->policy_count() << "\n"
              << "  claims         : " << get_sm()->claim_count()  << "\n";
}

static void cmd_list_servers() {
    std::vector<ptr<srv_config>> cfgs;
    g.raft_instance_->get_srv_config_all(cfgs);
    int leader = g.raft_instance_->get_leader();
    for (auto& c : cfgs) {
        std::cout << "  server " << c->get_id() << ": " << c->get_endpoint();
        if (c->get_id() == leader) std::cout << "  [LEADER]";
        std::cout << "\n";
    }
}

static void print_help() {
    std::cout <<
        "\n  -- InsureRaft Commands --\n"
        "\n  Data entry:\n"
        "    policy <id> <holder> <premium$>          Create a policy\n"
        "    update_policy <id> <new_premium$>        Update premium\n"
        "    cancel_policy <id>                       Cancel a policy\n"
        "    claim <id> <policy_id> <claimant> <amt$> [notes]  File a claim\n"
        "    approve <claim_id> [notes]               Approve a claim\n"
        "    deny    <claim_id> [notes]               Deny a claim\n"
        "    pay     <claim_id> <amount$>             Issue payment\n"
        "    endorse <policy_id> <text...>            Add endorsement\n"
        "\n  Query:\n"
        "    policies                                 List all policies\n"
        "    claims                                   List all claims\n"
        "    stat                                     Raft status\n"
        "    ls                                       List cluster members\n"
        "\n  Cluster:\n"
        "    add <server_id> <host:port>              Add a peer node\n"
        "\n  Other:\n"
        "    help                                     Show this help\n"
        "    q / exit                                 Shutdown\n\n";
}

// ---------------------------------------------------------------------------
// REPL
// ---------------------------------------------------------------------------

static bool handle(const std::vector<std::string>& t) {
    if (t.empty()) return true;
    const std::string& cmd = t[0];

    if (cmd == "q" || cmd == "exit") {
        g.launcher_.shutdown(5);
        g.reset();
        return false;
    } else if (cmd == "policy")         { cmd_policy(t); }
    else if (cmd == "update_policy")    { cmd_update_policy(t); }
    else if (cmd == "cancel_policy")    { cmd_cancel_policy(t); }
    else if (cmd == "claim")            { cmd_claim(t); }
    else if (cmd == "approve")          { cmd_approve(t); }
    else if (cmd == "deny")             { cmd_deny(t); }
    else if (cmd == "pay")              { cmd_pay(t); }
    else if (cmd == "endorse")          { cmd_endorse(t); }
    else if (cmd == "policies")         { get_sm()->print_policies(); }
    else if (cmd == "claims")           { get_sm()->print_claims(); }
    else if (cmd == "stat")             { cmd_stat(); }
    else if (cmd == "ls" || cmd == "list") { cmd_list_servers(); }
    else if (cmd == "add")              { cmd_add(t); }
    else if (cmd == "help" || cmd == "h")  { print_help(); }
    else                                { std::cout << "  unknown command (try 'help')\n"; }

    return true;
}

static void repl() {
    std::string prompt = "insure[" + std::to_string(g.server_id_) + "]> ";
    std::string line;
    while (true) {
        std::cout << prompt << std::flush;
        if (!std::getline(std::cin, line)) break;
        if (!handle(tokenize(line))) break;
    }
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

static void init_raft() {
    std::string log_file = g.data_dir_ + "/raft.log";
    ptr<logger_wrapper> log_wrap = cs_new<logger_wrapper>(log_file, 4);
    g.raft_logger_ = log_wrap;

    g.sm_   = cs_new<insurance_state_machine>();
    g.smgr_ = cs_new<insurance_state_mgr>(g.server_id_, g.endpoint_, g.data_dir_);

    asio_service::options asio_opt;
    asio_opt.thread_pool_size_ = 4;

    raft_params params;
    params.heart_beat_interval_       = 100;   // ms
    params.election_timeout_lower_bound_ = 200;
    params.election_timeout_upper_bound_ = 400;
    params.reserved_log_items_        = 20;
    params.snapshot_distance_         = 100;   // snapshot every 100 commits
    params.client_req_timeout_        = 3000;
    params.return_method_             = raft_params::blocking;

    g.raft_instance_ = g.launcher_.init(g.sm_, g.smgr_, g.raft_logger_,
                                        g.port_, asio_opt, params);
    if (!g.raft_instance_) {
        std::cerr << "Failed to initialize Raft instance.\n";
        exit(1);
    }

    const int MAX_TRIES = 20;
    std::cout << "Initializing Raft";
    for (int i = 0; i < MAX_TRIES; ++i) {
        if (g.raft_instance_->is_initialized()) {
            std::cout << " ready.\n";
            return;
        }
        std::cout << "." << std::flush;
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    std::cout << " TIMEOUT\n";
    exit(1);
}

static void usage(const char* prog) {
    std::cerr << "Usage: " << prog
              << " <server_id> <host:port> [data_dir]\n"
              << "  server_id  : unique integer >= 1\n"
              << "  host:port  : e.g. localhost:26000\n"
              << "  data_dir   : persistence directory (default: ./data/srv_<id>)\n\n";
    exit(1);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    if (argc < 3) usage(argv[0]);

    g.server_id_ = std::atoi(argv[1]);
    if (g.server_id_ < 1) usage(argv[0]);

    std::string ep = argv[2];
    size_t colon = ep.rfind(':');
    if (colon == std::string::npos) usage(argv[0]);
    g.port_     = std::atoi(ep.substr(colon + 1).c_str());
    g.addr_     = ep.substr(0, colon);
    g.endpoint_ = ep;

    if (argc >= 4) {
        g.data_dir_ = argv[3];
    } else {
        g.data_dir_ = "./data/srv_" + std::to_string(g.server_id_);
    }
    mkdir(g.data_dir_.c_str(), 0755);

    std::cout << "\n  *** InsureRaft — Distributed Insurance Data Transfer ***\n"
              << "  Server ID : " << g.server_id_ << "\n"
              << "  Endpoint  : " << g.endpoint_  << "\n"
              << "  Data dir  : " << g.data_dir_  << "\n\n";

    init_raft();
    print_help();
    repl();

    return 0;
}
