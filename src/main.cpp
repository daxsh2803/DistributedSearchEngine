// Distributed Search Engine - Application Entry Point (Phase 7A-2).
//
// Starts the HTTP server with document persistence.
// On startup, loads persisted documents and rebuilds the inverted index.
// If no persistence file exists, loads a deterministic seed corpus.
//
// Configuration (first match wins):
//   Port:
//     1. --port <number>   command-line argument
//     2. DSE_PORT=<number> environment variable
//     3. default: 8080
//   Data path:
//     1. --data <path>     command-line argument
//     2. DSE_DATA=<path>   environment variable
//     3. default: data/documents.jsonl
//
// Shutdown: press Ctrl+C (SIGINT) or send SIGTERM.

#include "document_store.h"
#include "http_server.h"
#include "ingestion_service.h"
#include "inverted_index.h"
#include "search_service.h"
#include "tokenizer.h"

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

// Global pointer to the server for signal handler access.
// Safe because only one HttpServer exists, and stop() is thread-safe.
std::atomic<dse::HttpServer*> g_server = nullptr;

void signal_handler(int /*signum*/)
{
    if (auto* srv = g_server.load()) {
        srv->stop();
    }
}

// ---------------------------------------------------------------------------
// Seed corpus — small, deterministic demo data for first-run.
// Only loaded when no persistence file exists.
// ---------------------------------------------------------------------------

void load_seed_corpus(dse::IngestionService& ingestion)
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
        ingestion.ingest({doc.id, std::string(doc.text)});
    }
}

// ---------------------------------------------------------------------------
// Rebuild the InvertedIndex from DocumentStore.
// This demonstrates that the index is derived state.
// ---------------------------------------------------------------------------

void rebuild_index(dse::InvertedIndex& index, const dse::DocumentStore& store)
{
    for (const auto& [id, doc] : store.all()) {
        index.add_document(id, doc.content);
    }
}

// ---------------------------------------------------------------------------
// Command-line / environment configuration
// ---------------------------------------------------------------------------

std::string resolve_data_path(int argc, char* argv[])
{
    // 1. Command-line argument
    for (int i = 1; i < argc - 1; ++i) {
        if (std::string_view(argv[i]) == "--data") {
            return argv[i + 1];
        }
    }

    // 2. Environment variable
    if (const char* env = std::getenv("DSE_DATA")) {
        return env;
    }

    // 3. Default
    return "data/documents.jsonl";
}

int resolve_port(int argc, char* argv[])
{
    // 1. Command-line argument
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

    // 2. Environment variable
    if (const char* env = std::getenv("DSE_PORT")) {
        try {
            const int p = std::stoi(env);
            if (p > 0) return p;
        } catch (...) {}
    }

    // 3. Default
    return 8080;
}

} // namespace

int main(int argc, char* argv[])
{
    std::cout << "Distributed Search Engine | Phase 7 - Document Persistence\n";
    std::cout << "Built with C++ standard: " << __cplusplus << "\n\n";

    // --- Resolve configuration ---
    const std::string data_path = resolve_data_path(argc, argv);
    std::cout << "Data path: " << data_path << "\n";

    // --- Create storage components ---
    dse::InvertedIndex index;
    dse::DocumentStore store;

    // --- Create the service layer (with persistence) ---
    dse::IngestionService ingestion(index, store, data_path);
    const dse::SearchService search(index);

    // --- Startup recovery: load persisted documents or seed corpus ---
    if (store.load(data_path)) {
        // Persistence file loaded successfully — rebuild index from documents.
        rebuild_index(index, store);
        std::cout << "Loaded " << index.document_count() << " persisted documents ("
                  << index.term_count() << " distinct terms)\n";
    } else {
        // No persistence file (first run) — load seed corpus.
        std::cout << "No persistence file found — loading seed corpus\n";
        load_seed_corpus(ingestion);
        std::cout << "Loaded " << index.document_count() << " seed documents ("
                  << index.term_count() << " distinct terms)\n";
    }

    // --- Determine the port ---
    const int port = resolve_port(argc, argv);
    if (port <= 0 || port > 65535) {
        std::cerr << "Invalid port: " << port << "\n";
        return 1;
    }

    // --- Start the HTTP server ---
    dse::HttpServer server(search, ingestion);
    g_server.store(&server);

    // Install signal handlers for clean shutdown
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

    // --- Wait for shutdown ---
    server_thread.join();
    g_server.store(nullptr);

    std::cout << "\nServer stopped.\n";
    return 0;
}
