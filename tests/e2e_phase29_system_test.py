#!/usr/bin/env python3
"""
Phase 29: System Validation E2E Orchestrator.

Orchestrates live Docker/Kafka lifecycle for the 3-node integrated system validation test:
1. Detects Docker daemon availability.
2. Starts Kafka via docker/docker-compose.kafka.yml.
3. Bounded polling for Kafka readiness using kafka-topics.sh.
4. Spawns tests/phase29_system_integration_test C++ binary.
5. Listens for deterministic IPC commands via stdout/stdin:
   - REQUEST: STOP_KAFKA  -> stops Kafka container, responds ACK: KAFKA_STOPPED
   - REQUEST: START_KAFKA -> starts Kafka container, polls readiness, responds ACK: KAFKA_READY
6. Collects C++ test results.
7. Guarantees Kafka restoration in finally block.
"""

import os
import sys
import time
import subprocess

COMPOSE_FILE = os.path.join("docker", "docker-compose.kafka.yml")

def run_command(cmd, timeout=60):
    try:
        res = subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout
        )
        return res.returncode, res.stdout.strip(), res.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", f"Command timed out after {timeout}s: {cmd}"
    except Exception as e:
        return -1, "", str(e)

def check_docker_available():
    code, _, _ = run_command("docker info", timeout=15)
    return code == 0

def check_kafka_health(timeout=45):
    start = time.time()
    while time.time() - start < timeout:
        code, out, _ = run_command(
            f"docker compose -f {COMPOSE_FILE} exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list",
            timeout=10
        )
        if code == 0:
            return True
        time.sleep(1)
    return False

def stop_kafka():
    code, _, err = run_command(f"docker compose -f {COMPOSE_FILE} stop kafka", timeout=30)
    return code == 0

def start_kafka():
    code, _, err = run_command(f"docker compose -f {COMPOSE_FILE} start kafka", timeout=30)
    if code != 0:
        return False
    return check_kafka_health(timeout=45)

def find_test_binary():
    # If passed as command line argument
    if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
        return os.path.abspath(sys.argv[1])
    
    candidates = [
        os.path.join("build_kafka", "phase29_system_integration_test.exe"),
        os.path.join("build_kafka", "phase29_system_integration_test"),
        os.path.join("build", "phase29_system_integration_test.exe"),
        os.path.join("build", "phase29_system_integration_test"),
    ]
    for cand in candidates:
        if os.path.isfile(cand):
            return os.path.abspath(cand)
    return None

def main():
    print("==================================================")
    print("Phase 29: Distributed Search Engine System Validation")
    print("==================================================")

    if not check_docker_available():
        print("[LIMITATION] Docker daemon is not running or not accessible.")
        print("RESULT: ENVIRONMENT_BLOCKED (Docker unavailable; run C++ tests in standalone mode).")
        return 0

    if not os.path.exists(COMPOSE_FILE):
        print(f"[ERROR] Compose file not found: {COMPOSE_FILE}")
        return 1

    binary_path = find_test_binary()
    if not binary_path:
        print("[ERROR] Could not find phase29_system_integration_test binary.")
        print("Please build the project with ENABLE_KAFKA=ON first.")
        return 1

    print(f"[ORCHESTRATOR] Found test binary: {binary_path}")

    # 1. Ensure Kafka is started and healthy
    print("[ORCHESTRATOR] Ensuring Kafka container is started and healthy...")
    code, _, err = run_command(f"docker compose -f {COMPOSE_FILE} up -d kafka", timeout=60)
    if code != 0:
        print(f"[LIMITATION] Unable to launch Docker Kafka: {err}")
        return 1

    if not check_kafka_health(timeout=45):
        print("[ERROR] Kafka failed readiness check within timeout.")
        return 1

    print("[ORCHESTRATOR] Kafka is healthy. Launching C++ system validation binary...")

    # 2. Launch C++ test binary with IPC environment
    env = os.environ.copy()
    env["DSE_PHASE29_ORCHESTRATOR"] = "1"
    env["DSE_KAFKA_ENABLED"] = "1"
    env["DSE_KAFKA_BROKERS"] = "localhost:9094"

    proc = subprocess.Popen(
        [binary_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env
    )

    test_failed = False
    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\r\n")
            print(line, flush=True)

            if line == "REQUEST: STOP_KAFKA":
                print("[ORCHESTRATOR] Received REQUEST: STOP_KAFKA. Stopping Kafka container...", flush=True)
                if not stop_kafka():
                    print("[ORCHESTRATOR ERROR] Failed to stop Kafka container!", flush=True)
                    test_failed = True
                    break
                proc.stdin.write("ACK: KAFKA_STOPPED\n")
                proc.stdin.flush()
                print("[ORCHESTRATOR] Sent ACK: KAFKA_STOPPED.", flush=True)

            elif line == "REQUEST: START_KAFKA":
                print("[ORCHESTRATOR] Received REQUEST: START_KAFKA. Restarting Kafka container...", flush=True)
                if not start_kafka():
                    print("[ORCHESTRATOR ERROR] Failed to start/verify Kafka container!", flush=True)
                    test_failed = True
                    break
                proc.stdin.write("ACK: KAFKA_READY\n")
                proc.stdin.flush()
                print("[ORCHESTRATOR] Sent ACK: KAFKA_READY.", flush=True)

        proc.wait()
    except Exception as e:
        print(f"[ORCHESTRATOR EXCEPTION] {e}", flush=True)
        test_failed = True
    finally:
        # Guarantee Kafka is restarted if still stopped
        print("[ORCHESTRATOR] Ensuring Kafka container is restored...", flush=True)
        try:
            start_kafka()
        except Exception:
            pass

    if test_failed or proc.returncode != 0:
        print(f"\nPhase 29 System Validation FAILED (exit code: {proc.returncode}).")
        return 1

    print("\nPhase 29 System Validation PASSED.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
