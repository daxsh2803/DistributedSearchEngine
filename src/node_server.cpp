// Distributed Search Engine - Node Server (Phase 12).
//
// HTTP POST endpoints wrapping LocalNode. Each endpoint deserializes
// the JSON request, calls the corresponding LocalNode method, and
// serializes the JSON response. Uses node_wire.h for serialization.

#include "node_server.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <cstddef>
#include <string>
#include <utility>

#include "local_node.h"
#include "node_wire.h"

namespace dse {

namespace {

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

NodeServer::NodeServer(std::size_t node_id, std::unique_ptr<LocalNode> local_node)
    : node_id_(node_id)
    , owned_node_(std::move(local_node))
    , local_node_(owned_node_.get())
    , server_(std::make_unique<httplib::Server>())
{
    register_routes();
}

NodeServer::NodeServer(std::size_t node_id, LocalNode* local_node)
    : node_id_(node_id)
    , owned_node_(nullptr)
    , local_node_(local_node)
    , server_(std::make_unique<httplib::Server>())
{
    register_routes();
}

NodeServer::~NodeServer()
{
    stop();
}

bool NodeServer::bind(int port)
{
    if (port == 0) {
        const int actual = server_->bind_to_any_port("127.0.0.1");
        if (actual < 0) return false;
        port_ = actual;
    } else {
        if (!server_->bind_to_port("127.0.0.1", port)) return false;
        port_ = port;
    }
    return true;
}

bool NodeServer::listen_after_bind()
{
    if (port_ <= 0) {
        server_->decommission();
        return false;
    }
    const bool ok = server_->listen_after_bind();
    if (!ok) {
        server_->decommission();
    }
    return ok;
}

bool NodeServer::listen(int port)
{
    if (!bind(port)) return false;
    return listen_after_bind();
}

void NodeServer::stop()
{
    if (server_) {
        server_->stop();
    }
}

void NodeServer::wait_until_ready() const
{
    server_->wait_until_ready();
}

bool NodeServer::is_running() const
{
    return server_ ? server_->is_running() : false;
}

int NodeServer::port() const
{
    return port_;
}

std::size_t NodeServer::node_id() const
{
    return node_id_;
}

// ---------------------------------------------------------------------------
// Route registration
// ---------------------------------------------------------------------------

void NodeServer::register_routes()
{
    // --- POST /node/search ---
    server_->Post("/node/search", [this](const httplib::Request& req,
                                          httplib::Response& res) {
        try {
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                error_response(res, 400, "Invalid JSON body");
                return;
            }

            ShardSearchRequest request;
            try {
                request = shard_search_request_from_json(body);
            } catch (const std::exception& e) {
                error_response(res, 400, std::string("Invalid request: ") + e.what());
                return;
            }

            auto response = local_node_->search(request);
            res.status = 200;
            res.set_content(to_json(response).dump(), "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });

    // --- POST /node/add ---
    server_->Post("/node/add", [this](const httplib::Request& req,
                                       httplib::Response& res) {
        try {
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                error_response(res, 400, "Invalid JSON body");
                return;
            }

            ShardWriteRequest request;
            try {
                request = shard_write_request_from_json(body);
            } catch (const std::exception& e) {
                error_response(res, 400, std::string("Invalid request: ") + e.what());
                return;
            }

            auto response = local_node_->add_document(request);

            if (response.is_error) {
                const bool is_conflict =
                    response.error_message.find("already exists") !=
                    std::string::npos;
                res.status = is_conflict ? 409 : 400;
            } else {
                res.status = 200;
            }
            res.set_content(to_json(response).dump(), "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });

    // --- POST /node/update ---
    server_->Post("/node/update", [this](const httplib::Request& req,
                                          httplib::Response& res) {
        try {
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                error_response(res, 400, "Invalid JSON body");
                return;
            }

            ShardWriteRequest request;
            try {
                request = shard_write_request_from_json(body);
            } catch (const std::exception& e) {
                error_response(res, 400, std::string("Invalid request: ") + e.what());
                return;
            }

            auto response = local_node_->update_document(request);

            if (response.is_error) {
                const bool is_not_found =
                    response.error_message.find("not found") !=
                    std::string::npos;
                res.status = is_not_found ? 404 : 400;
            } else {
                res.status = 200;
            }
            res.set_content(to_json(response).dump(), "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });

    // --- POST /node/remove ---
    server_->Post("/node/remove", [this](const httplib::Request& req,
                                          httplib::Response& res) {
        try {
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                error_response(res, 400, "Invalid JSON body");
                return;
            }

            ShardRemoveRequest request;
            try {
                request = shard_remove_request_from_json(body);
            } catch (const std::exception& e) {
                error_response(res, 400, std::string("Invalid request: ") + e.what());
                return;
            }

            auto response = local_node_->remove_document(request);

            if (response.is_error) {
                const bool is_not_found =
                    response.error_message.find("not found") !=
                    std::string::npos;
                res.status = is_not_found ? 404 : 400;
            } else {
                res.status = 200;
            }
            res.set_content(to_json(response).dump(), "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });

    // --- POST /node/get ---
    server_->Post("/node/get", [this](const httplib::Request& req,
                                       httplib::Response& res) {
        try {
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                error_response(res, 400, "Invalid JSON body");
                return;
            }

            ShardGetRequest request;
            try {
                request = shard_get_request_from_json(body);
            } catch (const std::exception& e) {
                error_response(res, 400, std::string("Invalid request: ") + e.what());
                return;
            }

            auto response = local_node_->get_document(request);
            res.status = 200;
            res.set_content(to_json(response).dump(), "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });

    // --- POST /node/count ---
    server_->Post("/node/count", [this](const httplib::Request& req,
                                         httplib::Response& res) {
        try {
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                error_response(res, 400, "Invalid JSON body");
                return;
            }

            ShardCountRequest request;
            try {
                request = shard_count_request_from_json(body);
            } catch (const std::exception& e) {
                error_response(res, 400, std::string("Invalid request: ") + e.what());
                return;
            }

            auto response = local_node_->document_count(request);
            res.status = 200;
            res.set_content(to_json(response).dump(), "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });

    // --- POST /node/save ---
    server_->Post("/node/save", [this](const httplib::Request& req,
                                        httplib::Response& res) {
        try {
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                error_response(res, 400, "Invalid JSON body");
                return;
            }

            std::size_t shard_id;
            try {
                shard_id = shard_persistence_request_from_json(body);
            } catch (const std::exception& e) {
                error_response(res, 400, std::string("Invalid request: ") + e.what());
                return;
            }

            bool ok = local_node_->save_shard(shard_id);
            res.status = 200;
            res.set_content(shard_persistence_response_to_json(ok).dump(),
                            "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });

    // --- POST /node/load ---
    server_->Post("/node/load", [this](const httplib::Request& req,
                                        httplib::Response& res) {
        try {
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (...) {
                error_response(res, 400, "Invalid JSON body");
                return;
            }

            std::size_t shard_id;
            try {
                shard_id = shard_persistence_request_from_json(body);
            } catch (const std::exception& e) {
                error_response(res, 400, std::string("Invalid request: ") + e.what());
                return;
            }

            bool ok = local_node_->load_shard(shard_id);
            res.status = 200;
            res.set_content(shard_persistence_response_to_json(ok).dump(),
                            "application/json");
        } catch (...) {
            error_response(res, 500, "Internal server error");
        }
    });
}

} // namespace dse
