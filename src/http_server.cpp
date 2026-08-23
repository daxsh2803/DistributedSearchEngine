// Distributed Search Engine - HTTP Server (Phase 5B-2, 9, 10).
//
// Implements the HTTP transport layer using cpp-httplib and nlohmann/json.
// Routes all requests through ShardCoordinator.
// GET /search, POST /documents, PUT /documents/:id, DELETE /documents/:id.
//
// PUT/DELETE use httplib's PathParamsMatcher (/documents/:id) which is
// portable across all platforms including MinGW/MSYS2.

#include "http_server.h"
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

HttpServer::HttpServer(ShardCoordinator& coordinator)
    : coordinator_(coordinator)
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
