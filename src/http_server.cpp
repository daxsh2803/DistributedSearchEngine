// Distributed Search Engine - HTTP Server (Phase 5B-2).
//
// Implements the HTTP transport layer using cpp-httplib and nlohmann/json.
// The single GET /search endpoint translates HTTP query parameters into
// a SearchRequest, delegates to SearchService, and serializes the response
// as JSON.

#include "http_server.h"
#include "ingestion_service.h"
#include "search_service.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <cstdlib>
#include <string>
#include <utility>

namespace dse {

namespace {

// Serialize a SearchResponse to a JSON string.
std::string to_json(const SearchResponse& resp)
{
    nlohmann::json j;

    if (resp.is_error) {
        j["error"] = resp.error_message;
        return j.dump();
    }

    j["query"] = resp.query;
    j["mode"]  = resp.mode;
    j["total"] = resp.total;
    j["limit"] = resp.limit;

    nlohmann::json results = nlohmann::json::array();
    for (const auto& r : resp.results) {
        results.push_back({
            {"document_id", r.document_id},
            {"score",       r.score}
        });
    }
    j["results"] = results;

    return j.dump();
}

} // namespace

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

HttpServer::HttpServer(const SearchService& search,
                       IngestionService& ingestion)
    : search_(search)
    , ingestion_(ingestion)
    , server_(std::make_unique<httplib::Server>())
{
    register_routes();
}

HttpServer::~HttpServer()
{
    stop();
}

bool HttpServer::listen(int port)
{
    if (port == 0) {
        // OS-assigned port (used by tests).
        const int actual = server_->bind_to_any_port("127.0.0.1");
        if (actual < 0) return false;
        port_ = actual;
    } else {
        // Specific port requested.
        if (!server_->bind_to_port("127.0.0.1", port)) return false;
        port_ = port;
    }
    // listen_after_bind() blocks the calling thread until stop() is called.
    return server_->listen_after_bind();
}

void HttpServer::stop()
{
    if (server_) {
        server_->stop();
    }
}

void HttpServer::wait_until_ready() const
{
    server_->wait_until_ready();
}

int HttpServer::port() const
{
    return port_;
}

// ---------------------------------------------------------------------------
// Route registration
// ---------------------------------------------------------------------------

void HttpServer::register_routes()
{
    // --- GET /search ---
    server_->Get("/search", [this](const httplib::Request& req,
                                    httplib::Response& res) {
        // --- Parse query parameters ---
        SearchRequest request;
        request.query = req.get_param_value("q");

        // Mode parameter
        const auto mode = req.get_param_value("mode");
        if (mode == "and") {
            request.mode = SearchMode::And;
        } else if (mode.empty() || mode == "or") {
            request.mode = SearchMode::Or;
        } else {
            res.status = 400;
            res.set_content(
                to_json(SearchResponse{
                    "", "", 0, 0, {}, true,
                    "Invalid mode: must be 'and' or 'or'"
                }),
                "application/json");
            return;
        }

        // Limit parameter
        if (!req.get_param_value("limit").empty()) {
            try {
                const auto val = std::stoi(req.get_param_value("limit"));
                if (val < 1 || val > 100) {
                    res.status = 400;
                    res.set_content(
                        to_json(SearchResponse{
                            "", "", 0, 0, {}, true,
                            "Invalid limit: must be between 1 and 100"
                        }),
                        "application/json");
                    return;
                }
                request.limit = static_cast<std::size_t>(val);
            } catch (...) {
                res.status = 400;
                res.set_content(
                    to_json(SearchResponse{
                        "", "", 0, 0, {}, true,
                        "Invalid limit: must be an integer"
                    }),
                    "application/json");
                return;
            }
        }

        // --- Execute search ---
        try {
            const auto response = search_.search(request);

            if (response.is_error) {
                res.status = 400;
                res.set_content(to_json(response), "application/json");
                return;
            }

            res.status = 200;
            res.set_content(to_json(response), "application/json");
        } catch (...) {
            res.status = 500;
            res.set_content(
                to_json(SearchResponse{
                    "", "", 0, 0, {}, true,
                    "Internal server error"
                }),
                "application/json");
        }
    });

    // --- POST /documents ---
    server_->Post("/documents", [this](const httplib::Request& req,
                                        httplib::Response& res) {
        try {
            // Parse JSON body
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                res.status = 400;
                nlohmann::json err;
                err["error"] = "Invalid JSON body";
                res.set_content(err.dump(), "application/json");
                return;
            }

            // Extract fields
            IngestDocumentRequest request;

            if (!body.contains("id") || !body["id"].is_number_unsigned()) {
                res.status = 400;
                nlohmann::json err;
                err["error"] = "Missing or invalid 'id' field (must be a non-negative integer)";
                res.set_content(err.dump(), "application/json");
                return;
            }
            request.id = body["id"].get<doc_id>();

            if (!body.contains("content") || !body["content"].is_string()) {
                res.status = 400;
                nlohmann::json err;
                err["error"] = "Missing or invalid 'content' field (must be a string)";
                res.set_content(err.dump(), "application/json");
                return;
            }
            request.content = body["content"].get<std::string>();

            // Delegate to IngestionService
            const auto response = ingestion_.ingest(request);

            if (response.is_error) {
                // Check if it's a conflict (duplicate ID) or validation error
                const bool is_conflict = response.error_message.find("already exists") != std::string::npos;
                res.status = is_conflict ? 409 : 400;
                nlohmann::json err;
                err["error"] = response.error_message;
                res.set_content(err.dump(), "application/json");
                return;
            }

            // Success: 201 Created
            nlohmann::json result;
            result["document_id"] = response.document_id;
            result["terms_indexed"] = response.terms_indexed;
            res.status = 201;
            res.set_content(result.dump(), "application/json");
        } catch (...) {
            res.status = 500;
            nlohmann::json err;
            err["error"] = "Internal server error";
            res.set_content(err.dump(), "application/json");
        }
    });
}

} // namespace dse
