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
#include "local_node.h"
#include "node_client.h"
#include "node_config.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

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

void signal_handler(int /*signum*/)
{
    if (auto* srv = g_server.load()) {
        srv->stop();
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

} // namespace

int main(int argc, char* argv[])
{
    std::cout << "Distributed Search Engine | Phase 11 - Node Abstraction\n";
    std::cout << "Built with C++ standard: " << __cplusplus << "\n\n";

    // --- Resolve configuration ---
    const std::string data_dir = resolve_data_dir(argc, argv);
    const std::size_t shard_count = resolve_shard_count(argc, argv);
    std::cout << "Data directory: " << data_dir << "\n";
    std::cout << "Shard count: " << shard_count << "\n";

    // --- Create router ---
    auto router = std::make_unique<dse::ShardRouter>(shard_count);

    // --- Create placement: all shards on node 0 (single-node default) ---
    std::vector<std::size_t> placement(shard_count, 0);
    auto shard_placement = std::make_unique<dse::ShardPlacement>(
        shard_count, 1, placement);

    // --- Create node with shards ---
    auto node = std::make_unique<dse::LocalNode>(0);
    for (std::size_t i = 0; i < shard_count; ++i) {
        const std::string path = data_dir + "shard-" + std::to_string(i)
                                + "/documents.jsonl";
        node->add_shard(i, std::make_unique<dse::Shard>(path));
    }

    std::vector<std::unique_ptr<dse::NodeClient>> nodes;
    nodes.push_back(std::move(node));

    // --- Create coordinator ---
    auto coordinator = std::make_unique<dse::ShardCoordinator>(
        std::move(router), std::move(shard_placement), std::move(nodes));

    // --- Startup recovery: load persisted documents or seed corpus ---
    bool loaded_persistence = false;

    // Try loading from persistence paths.
    coordinator->load_all();
    if (coordinator->total_document_count() > 0) {
        loaded_persistence = true;
    }

    if (loaded_persistence) {
        std::cout << "Loaded " << coordinator->total_document_count()
                  << " persisted documents\n";
    } else {
        std::cout << "No persistence found — loading seed corpus\n";
        load_seed_corpus(*coordinator);
        std::cout << "Loaded " << coordinator->total_document_count()
                  << " seed documents\n";
    }

    // --- Determine the port ---
    const int port = resolve_port(argc, argv);
    if (port <= 0 || port > 65535) {
        std::cerr << "Invalid port: " << port << "\n";
        return 1;
    }

    // --- Start the HTTP server ---
    dse::HttpServer server(*coordinator);
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

    server_thread.join();
    g_server.store(nullptr);

    std::cout << "\nServer stopped.\n";
    return 0;
}
