// Distributed Search Engine - HTTP Server (Phase 5B-2, 9, 10).
//
// Implements the HTTP transport layer using cpp-httplib and nlohmann/json.
// Routes all requests through ShardCoordinator.
// GET /search, POST /documents, PUT /documents/:id, DELETE /documents/:id.
//
// PUT/DELETE use httplib's PathParamsMatcher (/documents/:id) which is
// portable across all platforms including MinGW/MSYS2.

#include "http_server.h"
#include "metrics.h"
#include "shard_coordinator.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <cstdlib>
#include <string>
#include <utility>

namespace dse {

namespace {

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
    j["complete"] = resp.complete;

    nlohmann::json results = nlohmann::json::array();
    for (const auto& r : resp.results) {
        results.push_back({
            {"document_id", r.document_id},
            {"score",       r.score}
        });
    }
    j["results"] = results;

    if (!resp.complete && !resp.errors.empty()) {
        nlohmann::json errors = nlohmann::json::array();
        for (const auto& e : resp.errors) {
            errors.push_back({
                {"node_id",  e.node_id},
                {"shard_id", e.shard_id},
                {"category", e.category},
                {"message",  e.message}
            });
        }
        j["errors"] = errors;
    }

    return j.dump();
}

void error_response(httplib::Response& res, int status, const std::string& msg)
{
    res.status = status;
    nlohmann::json err;
    err["error"] = msg;
    res.set_content(err.dump(), "application/json");
}

} // namespace

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

HttpServer::HttpServer(ShardCoordinator& coordinator,
                       MetricsCollector* metrics)
    : coordinator_(coordinator)
    , metrics_(metrics)
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
        const int actual = server_->bind_to_any_port("127.0.0.1");
        if (actual < 0) return false;
        port_ = actual;
    } else {
        if (!server_->bind_to_port("127.0.0.1", port)) return false;
        port_ = port;
    }
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
    // --- GET /health ---
    server_->Get("/health", [](const httplib::Request& /*req*/,
                                httplib::Response& res) {
        res.status = 200;
        nlohmann::json j;
        j["status"] = "ok";
        res.set_content(j.dump(), "application/json");
    });

    // --- GET /metrics ---
    server_->Get("/metrics", [this](const httplib::Request& /*req*/,
                                     httplib::Response& res) {
        if (!metrics_) {
            res.status = 503;
            nlohmann::json err;
            err["error"] = "Metrics unavailable";
            res.set_content(err.dump(), "application/json");
            return;
        }

        const auto snap = metrics_->snapshot();
        nlohmann::json j;

        // Node-level search metrics
        j["searches_total"] = snap.searches_total;
        j["search_errors"] = snap.search_errors;
        j["search_incomplete"] = snap.search_incomplete;
        j["search_latency"] = {
            {"average_ms", snap.search_latency.average_ms},
            {"p99_ms", snap.search_latency.p99_ms},
            {"sample_count", snap.search_latency.sample_count}
        };

        // Node-level write metrics
        j["writes_total"] = snap.writes_total;
        j["write_errors"] = snap.write_errors;

        // Retry metrics
        j["retries_total"] = snap.retries_total;

        // Circuit breaker metrics
        j["circuit_open_events"] = snap.circuit_open_events;
        j["circuit_close_events"] = snap.circuit_close_events;

        // Coordinator-level search metrics
        j["coordinator_searches_total"] = snap.coordinator_searches_total;
        j["coordinator_search_success"] = snap.coordinator_search_success;
        j["coordinator_search_incomplete"] = snap.coordinator_search_incomplete;
        j["coordinator_search_errors"] = snap.coordinator_search_errors;
        j["coordinator_search_latency"] = {
            {"average_ms", snap.coordinator_search_latency.average_ms},
            {"p99_ms", snap.coordinator_search_latency.p99_ms},
            {"sample_count", snap.coordinator_search_latency.sample_count}
        };

        // Coordinator-level write metrics
        j["coordinator_writes_total"] = snap.coordinator_writes_total;
        j["coordinator_write_success"] = snap.coordinator_write_success;
        j["coordinator_write_errors"] = snap.coordinator_write_errors;
        j["coordinator_write_latency"] = {
            {"average_ms", snap.coordinator_write_latency.average_ms},
            {"p99_ms", snap.coordinator_write_latency.p99_ms},
            {"sample_count", snap.coordinator_write_latency.sample_count}
        };

        // Per-node metrics
        nlohmann::json per_node = nlohmann::json::object();
        for (const auto& [node_id, nm] : snap.per_node) {
            per_node[std::to_string(node_id)] = {
                {"searches", nm.searches},
                {"search_errors", nm.search_errors},
                {"search_incomplete", nm.search_incomplete},
                {"writes", nm.writes},
                {"write_errors", nm.write_errors},
                {"retries", nm.retries},
                {"circuit_state", static_cast<int>(nm.circuit_state)},
                {"circuit_open_events", nm.circuit_open_events},
                {"circuit_close_events", nm.circuit_close_events}
            };
        }
        j["per_node"] = per_node;

        res.status = 200;
        res.set_content(j.dump(), "application/json");
    });

    // --- GET /search ---
    server_->Get("/search", [this](const httplib::Request& req,
                                    httplib::Response& res) {
        SearchRequest request;
        request.query = req.get_param_value("q");

        const auto mode = req.get_param_value("mode");
        if (mode == "and") {
            request.mode = SearchMode::And;
        } else if (mode.empty() || mode == "or") {
            request.mode = SearchMode::Or;
        } else {
            error_response(res, 400, "Invalid mode: must be 'and' or 'or'");
            return;
        }

        if (!req.get_param_value("limit").empty()) {
            try {
                const auto val = std::stoi(req.get_param_value("limit"));
                if (val < 1 || val > 100) {
                    error_response(res, 400,
                        "Invalid limit: must be between 1 and 100");
                    return;
                }
                request.limit = static_cast<std::size_t>(val);
            } catch (...) {
                error_response(res, 400, "Invalid limit: must be an integer");
                return;
            }
        }

        try {
            const auto response = coordinator_.search(request);
            if (response.is_error) {
                error_response(res, 400, response.error_message);
                return;
            }
            res.status = 200;
            res.set_content(to_json(response), "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });

    // --- POST /documents ---
    server_->Post("/documents", [this](const httplib::Request& req,
                                        httplib::Response& res) {
        try {
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                error_response(res, 400, "Invalid JSON body");
                return;
            }

            if (!body.contains("id") || !body["id"].is_number_unsigned()) {
                error_response(res, 400,
                    "Missing or invalid 'id' field (must be a non-negative integer)");
                return;
            }

            if (!body.contains("content") || !body["content"].is_string()) {
                error_response(res, 400,
                    "Missing or invalid 'content' field (must be a string)");
                return;
            }

            CoordinatorIngestRequest request;
            request.id = body["id"].get<doc_id>();
            request.content = body["content"].get<std::string>();

            const auto response = coordinator_.ingest(request);

            if (response.is_error) {
                const bool is_conflict =
                    response.error_message.find("already exists") !=
                    std::string::npos;
                error_response(res, is_conflict ? 409 : 400,
                    response.error_message);
                return;
            }

            nlohmann::json result;
            result["document_id"] = response.document_id;
            result["terms_indexed"] = response.terms_indexed;
            res.status = 201;
            res.set_content(result.dump(), "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });

    // --- PUT /documents/:id ---
    server_->Put("/documents/:id", [this](const httplib::Request& req,
                                           httplib::Response& res) {
        try {
            const auto it = req.path_params.find("id");
            if (it == req.path_params.end()) {
                error_response(res, 400, "Missing document ID in URL");
                return;
            }

            doc_id id;
            try {
                id = static_cast<doc_id>(std::stoul(it->second));
            } catch (...) {
                error_response(res, 400, "Invalid document ID in URL");
                return;
            }

            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                error_response(res, 400, "Invalid JSON body");
                return;
            }

            if (!body.contains("content") || !body["content"].is_string()) {
                error_response(res, 400,
                    "Missing or invalid 'content' field (must be a string)");
                return;
            }

            CoordinatorUpdateRequest request;
            request.id = id;
            request.content = body["content"].get<std::string>();

            const auto response = coordinator_.update(request);

            if (response.is_error) {
                const bool is_not_found =
                    response.error_message.find("not found") !=
                    std::string::npos;
                error_response(res, is_not_found ? 404 : 400,
                    response.error_message);
                return;
            }

            nlohmann::json result;
            result["document_id"] = response.document_id;
            result["terms_indexed"] = response.terms_indexed;
            res.status = 200;
            res.set_content(result.dump(), "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });

    // --- DELETE /documents/:id ---
    server_->Delete("/documents/:id", [this](const httplib::Request& req,
                                              httplib::Response& res) {
        try {
            const auto it = req.path_params.find("id");
            if (it == req.path_params.end()) {
                error_response(res, 400, "Missing document ID in URL");
                return;
            }

            doc_id id;
            try {
                id = static_cast<doc_id>(std::stoul(it->second));
            } catch (...) {
                error_response(res, 400, "Invalid document ID in URL");
                return;
            }

            const auto response = coordinator_.remove(id);

            if (response.is_error) {
                const bool is_not_found =
                    response.error_message.find("not found") !=
                    std::string::npos;
                error_response(res, is_not_found ? 404 : 500,
                    response.error_message);
                return;
            }

            res.status = 204;
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });
}

} // namespace dse
