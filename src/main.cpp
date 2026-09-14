// Distributed Search Engine - Application Entry Point (Phase 7A-2, 10, 11).
//
// Starts the HTTP server with document persistence across N shards.
// On startup, loads persisted documents and rebuilds the inverted index
// for each shard. If no persistence file exists, loads a deterministic
// seed corpus.
//
// Configuration (first match wins):
//   Port:
//     1. --port <number>   command-line argument
//     2. DSE_PORT=<number> environment variable
//     3. default: 8080
//   Data path:
//     1. --data <path>     command-line argument
//     2. DSE_DATA=<path>   environment variable
//     3. default: data/
//   Shards:
//     1. --shards <number> command-line argument
//     2. DSE_SHARDS=<number> environment variable
//     3. default: 1
//
// Shutdown: press Ctrl+C (SIGINT) or send SIGTERM.

#include "http_server.h"
#include "in_memory_message_broker.h"
#include "local_node.h"
#include "metrics.h"
#include "node_client.h"
#include "node_config.h"
#include "node_server.h"
#include "persistent_event_store.h"
#include "remote_node.h"
#include "replica_placement.h"
#include "event_dispatcher.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

#ifdef DSE_KAFKA_ENABLED
#include "document_event.h"
#include "kafka_message_broker.h"
#include "remote_event_processor.h"
#endif

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

std::atomic<dse::HttpServer*> g_server = nullptr;
std::atomic<dse::NodeServer*> g_node_server = nullptr;

void signal_handler(int /*signum*/)
{
    if (auto* srv = g_server.load()) {
        srv->stop();
    }
    if (auto* ns = g_node_server.load()) {
        ns->stop();
    }
}

// ---------------------------------------------------------------------------
// Seed corpus
// ---------------------------------------------------------------------------

void load_seed_corpus(dse::ShardCoordinator& coord)
{
    struct Doc {
        dse::doc_id id;
        std::string_view text;
    };

    const std::vector<Doc> docs = {
        {  1, "The quick brown fox jumps over the lazy dog" },
        {  2, "A fast red fox leaps over a sleeping hound" },
        {  3, "The quick brown dog chases the lazy fox" },
        {  4, "C++ is a high-performance systems programming language" },
        {  5, "Rust is a systems language focused on memory safety" },
        {  6, "Python is a versatile scripting language" },
        {  7, "The Linux kernel is written in C" },
        {  8, "Git is a distributed version control system" },
        {  9, "Docker containers package applications for deployment" },
        { 10, "A search engine indexes documents for fast retrieval" },
        { 11, "Inverted maps map terms to document lists" },
        { 12, "TF-IDF scores term importance across documents" },
        { 13, "PostgreSQL is a relational database management system" },
        { 14, "Redis is an in-memory key-value store" },
        { 15, "Kafka handles high-throughput event streaming" },
        { 16, "Kubernetes orchestrates containerized applications" },
        { 17, "Machine learning models learn patterns from data" },
        { 18, "Neural networks are inspired by biological neurons" },
        { 19, "Web browsers render HTML and execute JavaScript" },
        { 20, "HTTP is the foundation of data communication on the web" },
    };

    for (const auto& doc : docs) {
        coord.ingest({doc.id, std::string(doc.text)});
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

std::string resolve_data_dir(int argc, char* argv[])
{
    for (int i = 1; i < argc - 1; ++i) {
        if (std::string_view(argv[i]) == "--data") {
            return argv[i + 1];
        }
    }
    if (const char* env = std::getenv("DSE_DATA")) {
        return env;
    }
    return "data/";
}

int resolve_port(int argc, char* argv[])
{
    for (int i = 1; i < argc - 1; ++i) {
        if (std::string_view(argv[i]) == "--port") {
            try {
                return std::stoi(argv[i + 1]);
            } catch (...) {
                std::cerr << "Invalid port: " << argv[i + 1] << "\n";
                return -1;
            }
        }
    }
    if (const char* env = std::getenv("DSE_PORT")) {
        try {
            const int p = std::stoi(env);
            if (p > 0) return p;
        } catch (...) {}
    }
    return 8080;
}

std::size_t resolve_shard_count(int argc, char* argv[])
{
    for (int i = 1; i < argc - 1; ++i) {
        if (std::string_view(argv[i]) == "--shards") {
            try {
                const int n = std::stoi(argv[i + 1]);
                if (n > 0) return static_cast<std::size_t>(n);
            } catch (...) {
                std::cerr << "Invalid shard count: " << argv[i + 1] << "\n";
                return 1;
            }
        }
    }
    if (const char* env = std::getenv("DSE_SHARDS")) {
        try {
            const int n = std::stoi(env);
            if (n > 0) return static_cast<std::size_t>(n);
        } catch (...) {}
    }
    return 1;
}

int resolve_node_id(int argc, char* argv[])
{
    for (int i = 1; i < argc; ++i) {
        if (std::string_view(argv[i]) == "--node-id") {
            if (i + 1 >= argc) {
                std::cerr << "Missing argument for --node-id\n";
                return -1;
            }
            try {
                std::size_t idx = 0;
                const std::string s = argv[i + 1];
                const int n = std::stoi(s, &idx);
                if (idx == s.size() && n >= 0) return n;
                std::cerr << "Invalid node ID: " << argv[i + 1] << "\n";
                return -1;
            } catch (...) {
                std::cerr << "Invalid node ID: " << argv[i + 1] << "\n";
                return -1;
            }
        }
    }
    if (const char* env = std::getenv("DSE_NODE_ID")) {
        try {
            std::size_t idx = 0;
            const std::string s = env;
            const int n = std::stoi(s, &idx);
            if (idx == s.size() && n >= 0) return n;
            std::cerr << "Invalid DSE_NODE_ID: " << env << "\n";
            return -1;
        } catch (...) {
            std::cerr << "Invalid DSE_NODE_ID: " << env << "\n";
            return -1;
        }
    }
    return 0;
}

int resolve_rpc_port(int argc, char* argv[])
{
    for (int i = 1; i < argc; ++i) {
        if (std::string_view(argv[i]) == "--rpc-port") {
            if (i + 1 >= argc) {
                std::cerr << "Missing argument for --rpc-port\n";
                return -1;
            }
            try {
                std::size_t idx = 0;
                const std::string s = argv[i + 1];
                const int p = std::stoi(s, &idx);
                if (idx == s.size() && p >= 0 && p <= 65535) return p;
                std::cerr << "Invalid RPC port: " << argv[i + 1] << "\n";
                return -1;
            } catch (...) {
                std::cerr << "Invalid RPC port: " << argv[i + 1] << "\n";
                return -1;
            }
        }
    }
    if (const char* env = std::getenv("DSE_RPC_PORT")) {
        try {
            std::size_t idx = 0;
            const std::string s = env;
            const int p = std::stoi(s, &idx);
            if (idx == s.size() && p >= 0 && p <= 65535) return p;
            std::cerr << "Invalid DSE_RPC_PORT: " << env << "\n";
            return -1;
        } catch (...) {
            std::cerr << "Invalid DSE_RPC_PORT: " << env << "\n";
            return -1;
        }
    }
    return 0;
}

std::string resolve_peers(int argc, char* argv[])
{
    for (int i = 1; i < argc - 1; ++i) {
        if (std::string_view(argv[i]) == "--peers") {
            return argv[i + 1];
        }
    }
    if (const char* env = std::getenv("DSE_PEERS")) {
        return env;
    }
    return "";
}

std::size_t resolve_replica_factor(int argc, char* argv[])
{
    for (int i = 1; i < argc - 1; ++i) {
        if (std::string_view(argv[i]) == "--replica-factor") {
            try {
                const int n = std::stoi(argv[i + 1]);
                if (n > 0) return static_cast<std::size_t>(n);
            } catch (...) {
                std::cerr << "Invalid replica factor: " << argv[i + 1] << "\n";
                return 1;
            }
        }
    }
    if (const char* env = std::getenv("DSE_REPLICATION_FACTOR")) {
        try {
            const int n = std::stoi(env);
            if (n > 0) return static_cast<std::size_t>(n);
        } catch (...) {}
    }
    return 1;
}

} // namespace

int main(int argc, char* argv[])
{
    std::cout << "Distributed Search Engine | Phase 18 - Async Messaging Pipeline\n";
    std::cout << "Built with C++ standard: " << __cplusplus << "\n\n";

    // --- Resolve configuration ---
    const std::string data_dir = resolve_data_dir(argc, argv);
    const std::size_t shard_count = resolve_shard_count(argc, argv);
    const int node_id_arg = resolve_node_id(argc, argv);
    if (node_id_arg < 0) {
        return 1;
    }
    const std::size_t local_node_id = static_cast<std::size_t>(node_id_arg);

    const int rpc_port_arg = resolve_rpc_port(argc, argv);
    if (rpc_port_arg < 0) {
        return 1;
    }
    const std::string peers_spec = resolve_peers(argc, argv);
    const std::size_t replica_factor = resolve_replica_factor(argc, argv);

    std::cout << "Data directory: " << data_dir << "\n";
    std::cout << "Shard count: " << shard_count << "\n";
    std::cout << "Node ID: " << local_node_id << "\n";

    // --- Create router ---
    auto router = std::make_unique<dse::ShardRouter>(shard_count);

    std::unique_ptr<dse::LocalNode> local_node;
    dse::LocalNode* local_node_ptr = nullptr;
    std::vector<std::unique_ptr<dse::NodeClient>> nodes;
    std::unique_ptr<dse::ShardCoordinator> coordinator;
    int rpc_port = rpc_port_arg;
    std::size_t node_count = 1;

    if (!peers_spec.empty()) {
        std::vector<dse::NodeEndpoint> peers;
        try {
            peers = dse::parse_peer_topology(peers_spec);
        } catch (const std::exception& e) {
            std::cerr << "Failed to parse peer topology: " << e.what() << "\n";
            return 1;
        }

        node_count = peers.size();

        if (local_node_id >= node_count || peers[local_node_id].node_id != local_node_id) {
            std::cerr << "Error: local node ID " << local_node_id
                      << " does not exist in the configured peer topology ("
                      << node_count << " nodes configured)\n";
            return 1;
        }

        if (rpc_port == 0) {
            rpc_port = peers[local_node_id].port;
        }

        if (replica_factor == 0 || replica_factor > node_count) {
            std::cerr << "Error: replication factor " << replica_factor
                      << " must be in [1, " << node_count << "]\n";
            return 1;
        }

        std::cout << "Node count (peers): " << node_count << "\n";
        std::cout << "Replication factor: " << replica_factor << "\n";
        std::cout << "RPC port: " << rpc_port << "\n";

        const auto replica_sets = dse::make_deterministic_replica_sets(
            shard_count, node_count, replica_factor);

        local_node = std::make_unique<dse::LocalNode>(local_node_id);
        for (const auto& rs : replica_sets) {
            bool is_replica = false;
            for (std::size_t nid : rs.node_ids) {
                if (nid == local_node_id) {
                    is_replica = true;
                    break;
                }
            }
            if (is_replica) {
                const std::string path = data_dir + "shard-" + std::to_string(rs.shard_id)
                                        + "/documents.jsonl";
                local_node->add_shard(rs.shard_id, std::make_unique<dse::Shard>(path));
            }
        }
        // local_node_ptr is non-owning and remains valid because ownership of
        // local_node is transferred into the nodes vector and then into ShardCoordinator,
        // whose lifetime extends until after NodeServer is stopped and joined.
        local_node_ptr = local_node.get();

        nodes.reserve(node_count);
        for (std::size_t i = 0; i < node_count; ++i) {
            if (i == local_node_id) {
                nodes.push_back(std::move(local_node));
            } else {
                nodes.push_back(std::make_unique<dse::RemoteNode>(
                    peers[i].node_id, peers[i].host, peers[i].port));
            }
        }

        auto replica_placement = std::make_unique<dse::ShardReplicaPlacement>(
            shard_count, node_count, replica_factor, replica_sets);

        coordinator = std::make_unique<dse::ShardCoordinator>(
            std::move(router), std::move(replica_placement), std::move(nodes));

    } else {
        // Single-node default: all shards on node 0
        std::vector<std::size_t> placement(shard_count, 0);
        auto shard_placement = std::make_unique<dse::ShardPlacement>(
            shard_count, 1, placement);

        local_node = std::make_unique<dse::LocalNode>(0);
        for (std::size_t i = 0; i < shard_count; ++i) {
            const std::string path = data_dir + "shard-" + std::to_string(i)
                                    + "/documents.jsonl";
            local_node->add_shard(i, std::make_unique<dse::Shard>(path));
        }

        // local_node_ptr is non-owning and remains valid because ownership is
        // transferred into nodes and ShardCoordinator, which outlive NodeServer.
        local_node_ptr = local_node.get();
        nodes.push_back(std::move(local_node));

        coordinator = std::make_unique<dse::ShardCoordinator>(
            std::move(router), std::move(shard_placement), std::move(nodes));
    }

    // --- Create metrics collector (shared by coordinator and HTTP server) ---
    dse::MetricsCollector metrics;

    // --- Create event system (Phase 18) ---
    // PersistentEventStore: durable outbox for event lifecycle tracking.
    // Recovers PENDING/FAILED events from previous runs on construction.
    dse::PersistentEventStore eventStore(data_dir + "events");

    coordinator->set_metrics(&metrics);
    coordinator->set_event_store(&eventStore);

    // --- Create event broker (Kafka-aware when DSE_KAFKA_ENABLED) ---
#ifdef DSE_KAFKA_ENABLED
    // Kafka path: node-specific consumer group so each node receives
    // the full event stream independently.
    dse::KafkaBrokerConfig kafkaConfig;
    kafkaConfig.bootstrap_servers = "localhost:9094";
    kafkaConfig.client_id = "dse-producer";
    kafkaConfig.group_id = "dse-node-" + std::to_string(local_node_id);
    kafkaConfig.consumer_client_id = "dse-consumer-" + std::to_string(local_node_id);
    kafkaConfig.auto_offset_reset = "earliest";

    // RemoteEventProcessor: applies remote mutations from Kafka.
    // Declared before broker so it outlives the consumer thread (strict RAII).
    // Own node ID = this node's ID so self-events are skipped.
    std::unordered_map<std::size_t, dse::LocalNode*> node_map;
    node_map[local_node_id] = local_node_ptr;
    dse::RemoteEventProcessor remoteProcessor(std::move(node_map), local_node_id);

    dse::KafkaMessageBroker broker(kafkaConfig);

    // Register handler for the single document mutations topic.
    broker.subscribe(dse::topics::kDocumentMutations,
        [&remoteProcessor](const dse::Message& msg) {
            return remoteProcessor.process(msg.topic, msg.payload);
        });

    // EventDispatcher: bounded async dispatch with retry support.
    dse::EventDispatcher::Config dispatcherConfig;
    dispatcherConfig.max_retries = 3;
    dispatcherConfig.retry_delay_ms = 100;
    dse::EventDispatcher dispatcher(broker, eventStore, dispatcherConfig);
#else
    // In-memory path (Phase 18): preserved for Kafka-disabled builds.
    dse::BrokerConfig brokerConfig;
    brokerConfig.max_queue_size = 4096;
    brokerConfig.consumer_threads = 2;
    brokerConfig.enable_idempotency = true;
    dse::InMemoryMessageBroker broker(brokerConfig);

    // EventDispatcher: bounded async dispatch with retry support.
    dse::EventDispatcher::Config dispatcherConfig;
    dispatcherConfig.max_retries = 3;
    dispatcherConfig.retry_delay_ms = 100;
    dse::EventDispatcher dispatcher(broker, eventStore, dispatcherConfig);
#endif

    coordinator->set_event_dispatcher(&dispatcher);

    // --- Startup recovery: load persisted documents or seed corpus ---
    bool loaded_persistence = false;
    std::size_t local_persisted_docs = 0;

    // Recover locally hosted persisted shards without remote RPCs.
    if (local_node_ptr) {
        for (std::size_t sid = 0; sid < shard_count; ++sid) {
            if (local_node_ptr->has_shard(sid)) {
                local_node_ptr->load_shard(sid);
                const auto count_resp = local_node_ptr->document_count(dse::ShardCountRequest{sid});
                if (!count_resp.is_error) {
                    local_persisted_docs += count_resp.document_count;
                }
            }
        }
    }
    if (local_persisted_docs > 0) {
        loaded_persistence = true;
    }

    if (loaded_persistence) {
        std::cout << "Loaded " << local_persisted_docs
                  << " persisted documents\n";
    } else if (node_count == 1) {
        std::cout << "No persistence found — loading seed corpus\n";
        load_seed_corpus(*coordinator);

        std::cout << "Loaded " << coordinator->total_document_count()
                  << " seed documents\n";
    } else {
        std::cout << "Multi-node cluster (" << node_count
                  << " nodes) — starting with clean cluster state\n";
    }

    // --- Determine the public port ---
    const int port = resolve_port(argc, argv);
    if (port <= 0 || port > 65535) {
        std::cerr << "Invalid port: " << port << "\n";
        return 1;
    }

    // --- Start NodeServer (RPC) if configured ---
    std::unique_ptr<dse::NodeServer> node_server;
    std::thread node_server_thread;
    if (rpc_port > 0) {
        node_server = std::make_unique<dse::NodeServer>(local_node_id, local_node_ptr);
        g_node_server.store(node_server.get());

        if (!node_server->bind(rpc_port)) {
            std::cerr << "Failed to bind NodeServer on RPC port " << rpc_port << "\n";
            g_node_server.store(nullptr);
            return 1;
        }

        std::atomic<bool> node_server_failed{false};
        node_server_thread = std::thread([&node_server, &node_server_failed]() {
            if (!node_server->listen_after_bind()) {
                node_server_failed.store(true);
            }
        });

        node_server->wait_until_ready();
        if (node_server_failed.load() || !node_server->is_running()) {
            std::cerr << "Failed to start NodeServer listener on RPC port " << rpc_port << "\n";
            g_node_server.store(nullptr);
            node_server->stop();
            if (node_server_thread.joinable()) {
                node_server_thread.join();
            }
            return 1;
        }

        std::cout << "NodeServer listening on http://127.0.0.1:" << node_server->port() << " (RPC)\n";
    }

    // --- Start the event system ---
    dispatcher.start();
#ifdef DSE_KAFKA_ENABLED
    broker.start();
#endif
    std::cout << "Event system started\n";

    // --- Start the HTTP server ---
    dse::HttpServer server(*coordinator, &metrics, &eventStore);
    g_server.store(&server);

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::thread server_thread([&server, port]() {
        if (!server.listen(port)) {
            std::cerr << "Failed to start server on port " << port << "\n";
            g_server.store(nullptr);
            return;
        }
    });

    server.wait_until_ready();
    const int actual_port = server.port();

    std::cout << "Server listening on http://127.0.0.1:" << actual_port << "\n";
    std::cout << "Try: curl \"http://127.0.0.1:" << actual_port
              << "/search?q=quick+fox\"\n";
    std::cout << "\nPress Ctrl+C to stop.\n";
    std::cout << "Try: curl \"http://127.0.0.1:" << actual_port
              << "/metrics\"\n";

    server_thread.join();
    g_server.store(nullptr);

    // Stop and join NodeServer while LocalNode and coordinator are still alive
    if (node_server) {
        node_server->stop();
        if (node_server_thread.joinable()) {
            node_server_thread.join();
        }
        g_node_server.store(nullptr);
    }

    // --- Shutdown event system in correct order ---
    // 1. Stop accepting new events and drain the queue
    dispatcher.stop();
    // 2. Persist any remaining events to disk
    eventStore.flush();
    // 3. Stop the message broker (Kafka consumer + producer, or in-memory)
#ifdef DSE_KAFKA_ENABLED
    broker.stop();
#endif

    std::cout << "\nServer stopped.\n";
    return 0;
}
