// Distributed Search Engine - Node Configuration (Phase 11).
//
// Implementation of the contract in src/node_config.h.
// Validates shard placement and provides deterministic shard→node routing.

#include "node_config.h"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace dse {

namespace {

std::string_view trim_sv(std::string_view s)
{
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
        s.remove_prefix(1);
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
        s.remove_suffix(1);
    }
    return s;
}

} // namespace

std::vector<NodeEndpoint> parse_peer_topology(std::string_view spec)
{
    spec = trim_sv(spec);
    if (spec.empty()) {
        throw std::invalid_argument("Empty peer topology specification");
    }

    std::vector<NodeEndpoint> endpoints;

    std::size_t start = 0;
    while (start < spec.size()) {
        const std::size_t comma = spec.find(',', start);
        std::string_view token = (comma == std::string_view::npos)
            ? spec.substr(start)
            : spec.substr(start, comma - start);
        start = (comma == std::string_view::npos) ? spec.size() : comma + 1;

        token = trim_sv(token);
        if (token.empty()) {
            throw std::invalid_argument("Empty token in peer topology");
        }

        // Expected format: <node_id>=<host>:<port>
        const std::size_t eq = token.find('=');
        if (eq == std::string_view::npos || eq == 0 || eq == token.size() - 1) {
            throw std::invalid_argument(
                "Invalid peer format (expected id=host:port): " + std::string(token));
        }

        const std::string_view id_sv = trim_sv(token.substr(0, eq));
        const std::string_view addr_sv = trim_sv(token.substr(eq + 1));

        std::size_t node_id = 0;
        try {
            std::size_t idx = 0;
            const std::string id_str(id_sv);
            const unsigned long val = std::stoul(id_str, &idx);
            if (idx != id_str.size()) {
                throw std::invalid_argument("trailing characters");
            }
            node_id = static_cast<std::size_t>(val);
        } catch (...) {
            throw std::invalid_argument("Invalid node ID in peer topology: " + std::string(id_sv));
        }

        const std::size_t colon = addr_sv.rfind(':');
        if (colon == std::string_view::npos || colon == 0 || colon == addr_sv.size() - 1) {
            throw std::invalid_argument(
                "Invalid host:port in peer topology: " + std::string(addr_sv));
        }

        const std::string host(trim_sv(addr_sv.substr(0, colon)));
        const std::string_view port_sv = trim_sv(addr_sv.substr(colon + 1));

        if (host.empty()) {
            throw std::invalid_argument("Empty host in peer topology");
        }

        auto is_valid_host_char = [](char c) {
            const unsigned char uc = static_cast<unsigned char>(c);
            return std::isalnum(uc) || c == '.' || c == '-' || c == '_' || c == '[' || c == ']' || c == ':';
        };
        for (char c : host) {
            if (!is_valid_host_char(c)) {
                throw std::invalid_argument("Invalid host in peer topology: " + host);
            }
        }

        int port = 0;
        try {
            std::size_t idx = 0;
            const std::string port_str(port_sv);
            const int p = std::stoi(port_str, &idx);
            if (idx != port_str.size() || p <= 0 || p > 65535) {
                throw std::invalid_argument("out of range");
            }
            port = p;
        } catch (...) {
            throw std::invalid_argument("Invalid port in peer topology: " + std::string(port_sv));
        }

        endpoints.push_back({node_id, host, port});
    }

    if (endpoints.empty()) {
        throw std::invalid_argument("No peer endpoints found");
    }

    // Sort by node_id
    std::sort(endpoints.begin(), endpoints.end(),
              [](const NodeEndpoint& a, const NodeEndpoint& b) {
                  return a.node_id < b.node_id;
              });

    // Validate strictly contiguous {0, 1, ..., N-1} and no duplicates
    for (std::size_t i = 0; i < endpoints.size(); ++i) {
        if (endpoints[i].node_id != i) {
            throw std::invalid_argument(
                "Peer node IDs must be strictly contiguous {0, 1, ..., N-1}. Expected " +
                std::to_string(i) + " but found " + std::to_string(endpoints[i].node_id));
        }
    }

    return endpoints;
}

std::vector<ShardReplicaSet> make_deterministic_replica_sets(
    std::size_t shard_count,
    std::size_t node_count,
    std::size_t replication_factor)
{
    if (shard_count == 0) {
        throw std::invalid_argument("make_deterministic_replica_sets: shard_count must be > 0");
    }
    if (node_count == 0) {
        throw std::invalid_argument("make_deterministic_replica_sets: node_count must be > 0");
    }
    if (replication_factor == 0 || replication_factor > node_count) {
        throw std::invalid_argument(
            "make_deterministic_replica_sets: replication_factor must be in [1, node_count]");
    }

    std::vector<ShardReplicaSet> replica_sets;
    replica_sets.reserve(shard_count);

    for (std::size_t sid = 0; sid < shard_count; ++sid) {
        std::vector<std::size_t> node_ids;
        node_ids.reserve(replication_factor);
        for (std::size_t r = 0; r < replication_factor; ++r) {
            node_ids.push_back((sid + r) % node_count);
        }
        replica_sets.push_back({sid, std::move(node_ids)});
    }

    return replica_sets;
}

ShardPlacement::ShardPlacement(std::size_t shard_count,
                               std::size_t node_count,
                               const std::vector<std::size_t>& placement)
    : shard_count_(shard_count)
    , node_count_(node_count)
    , shard_to_node_(shard_count)
{
    if (shard_count == 0) {
        throw std::invalid_argument("ShardPlacement: shard_count must be > 0");
    }
    if (node_count == 0) {
        throw std::invalid_argument("ShardPlacement: node_count must be > 0");
    }
    if (placement.size() != shard_count) {
        throw std::invalid_argument(
            "ShardPlacement: placement size (" +
            std::to_string(placement.size()) +
            ") must equal shard_count (" +
            std::to_string(shard_count) + ")");
    }

    for (std::size_t i = 0; i < shard_count; ++i) {
        if (placement[i] >= node_count) {
            throw std::invalid_argument(
                "ShardPlacement: shard " + std::to_string(i) +
                " references unknown node " + std::to_string(placement[i]));
        }
        shard_to_node_[i] = placement[i];
    }
}

std::size_t ShardPlacement::node_of(std::size_t shard_id) const
{
    return shard_to_node_.at(shard_id);
}

std::size_t ShardPlacement::shard_count() const
{
    return shard_count_;
}

std::size_t ShardPlacement::node_count() const
{
    return node_count_;
}

} // namespace dse
