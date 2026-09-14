#!/usr/bin/env pwsh
# Phase 21 Benchmark I — Kafka Unavailability: Write-Path Resilience Orchestrator
#
# Goal:
#   Measure write-path resilience when the Kafka broker is unavailable.
#   Synchronous RF=3 replication is authoritative. Kafka failure must NOT
#   affect HTTP 201 write success.
#
# Architecture:
#   Node 0 HTTP write
#     -> ShardCoordinator.ingest()
#     -> synchronous Phase 17 RF=3 replication (authoritative)
#     -> HTTP 201
#     -> EventStore / EventDispatcher background thread
#     -> Kafka broker (stopped in Phase 2, restarted in Phase 3)
#
# Safety & Verification Invariants:
#   - Never delete or overwrite existing benchmark results automatically.
#   - Refuse execution if target CSVs already exist.
#   - Refuse execution if data root is not fresh.
#   - Do NOT attempt to replay the 50 Phase 2 failed events (no HTTP replay endpoint).
#   - Recovery assertion: recovery_failed_delta == 0 (zero new failures during Phase 3).
#   - Final failed count expected: baseline_failed + 50.
#   - Cleanly terminate all cluster nodes upon completion or error.

param (
    [string]$RunId              = "I_kafka_unavail",
    [string]$DataRoot           = "data\phase21\I_kafka_unavail",
    [int]$Warmup                = 10,
    [int]$Measured              = 50,
    [int]$Recovery              = 10,
    [double]$DrainBufferSec     = 5.0,
    [double]$KafkaTimeoutSec    = 60.0,
    [double]$RecoveryDrainSec   = 30.0,
    [double]$CliTimeoutSec      = 10.0,
    [string]$ResultsDir         = "results\phase21_I_kafka_unavail",
    [string]$DseExe             = "build_kafka\DistributedSearchEngine.exe"
)

Set-StrictMode -Off
$ErrorActionPreference = "Stop"

$PSScriptRoot_Or_Benchmarks = if ($PSScriptRoot) { $PSScriptRoot } else { "benchmarks" }
$BENCH_PY = Join-Path $PSScriptRoot_Or_Benchmarks "bench_I.py"
if (-not (Test-Path $BENCH_PY)) {
    $BENCH_PY = "benchmarks\bench_I.py"
}

$PEERS  = "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083"
$SHARDS = "3"
$RF     = "3"

$SummaryResultPath   = Join-Path $ResultsDir "phase21_${RunId}_summary.csv"
$WritesResultPath    = Join-Path $ResultsDir "phase21_${RunId}_writes.csv"
$MetricsResultPath   = Join-Path $ResultsDir "phase21_${RunId}_metrics_timeline.csv"
$OutPrefix           = Join-Path $ResultsDir "phase21_${RunId}"

# ---------------------------------------------------------------------------
# 1. UPFRONT FRESHNESS & SAFETY CHECKS
# ---------------------------------------------------------------------------

Write-Host "================================================================"
Write-Host "Benchmark I: Pre-Flight Safety & Freshness Verification"
Write-Host "================================================================"

# A. Safety Check: Never delete or overwrite an existing benchmark result automatically
New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null
if ((Test-Path $SummaryResultPath) -or (Test-Path $WritesResultPath) -or (Test-Path $MetricsResultPath)) {
    Write-Error "ERROR: Target benchmark result already exists ($SummaryResultPath, $WritesResultPath, or $MetricsResultPath). Never delete or overwrite existing benchmark results automatically. Stop."
    exit 1
}

# B. Freshness Check: Verify data root does not contain stale benchmark data
$staleDirs = @()
foreach ($nodeId in @(0, 1, 2)) {
    $nodeDir = Join-Path $DataRoot "node$nodeId"
    if (Test-Path $nodeDir) {
        $items = Get-ChildItem -Path $nodeDir -Force -ErrorAction SilentlyContinue
        if ($items -and $items.Count -gt 0) {
            $staleDirs += $nodeDir
        }
    }
}
if ($staleDirs.Count -gt 0) {
    Write-Error "ERROR: Data directories already contain benchmark data (not fresh): $($staleDirs -join ', '). Fresh data root required. Do NOT delete automatically."
    exit 1
}

# C. Verify Kafka-enabled executable exists
if (-not (Test-Path $DseExe)) {
    if (Test-Path "build\Debug\DistributedSearchEngine.exe") {
        $checkKafka = Select-String -Path "build\Debug\DistributedSearchEngine.exe" -Pattern "dse-consumer" -Quiet
        if ($checkKafka) {
            $DseExe = "build\Debug\DistributedSearchEngine.exe"
        }
    }
}
if (-not (Test-Path $DseExe)) {
    Write-Error "ERROR: Kafka-enabled DistributedSearchEngine executable not found at '$DseExe'. Please ensure project is built with -DENABLE_KAFKA=ON (build_kafka)."
    exit 1
}

$isKafkaEnabled = Select-String -Path $DseExe -Pattern "dse-consumer" -Quiet
if (-not $isKafkaEnabled) {
    Write-Error "ERROR: Executable '$DseExe' was NOT built with Kafka enabled (missing dse-consumer symbol). Build with -DENABLE_KAFKA=ON required."
    exit 1
}
Write-Host "  Kafka-enabled binary: $DseExe (Verified)"

# ---------------------------------------------------------------------------
# 2. HELPER FUNCTIONS
# ---------------------------------------------------------------------------

function Kill-DSE {
    $procs = Get-Process -Name DistributedSearchEngine -ErrorAction SilentlyContinue
    foreach ($p in $procs) {
        Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Milliseconds 800
    $leftover = Get-Process -Name DistributedSearchEngine -ErrorAction SilentlyContinue
    if ($leftover) {
        Write-Error "DistributedSearchEngine process still alive after kill attempt."
        exit 1
    }
}

function Check-Port([string]$hostName, [int]$port, [int]$timeoutMs=2000) {
    $conn = New-Object System.Net.Sockets.TcpClient
    try {
        $asyncResult = $conn.BeginConnect($hostName, $port, $null, $null)
        if (-not $asyncResult.AsyncWaitHandle.WaitOne($timeoutMs, $false)) {
            $conn.Close()
            return $false
        }
        $conn.EndConnect($asyncResult)
        return $conn.Connected
    } catch {
        return $false
    } finally {
        $conn.Close()
    }
}

function Wait-Health([int]$port, [int]$maxRetries=100) {
    for ($i = 0; $i -lt $maxRetries; $i++) {
        try {
            $r = Invoke-WebRequest -Uri "http://127.0.0.1:$port/health" -UseBasicParsing -TimeoutSec 1
            if ($r.StatusCode -eq 200) { return $true }
        } catch {}
        Start-Sleep -Milliseconds 300
    }
    return $false
}

function Start-Cluster([object[]]$nodeList) {
    $procs = @()
    foreach ($n in $nodeList) {
        $stdoutF = "$($n.data)\stdout_I.log"
        $stderrF = "$($n.data)\stderr_I.log"
        $args = "--node-id $($n.id) --port $($n.http) --rpc-port $($n.rpc) --peers `"$PEERS`" --shards $SHARDS --replica-factor $RF --data $($n.data)/"
        $proc = Start-Process -FilePath $DseExe -ArgumentList $args -PassThru -RedirectStandardOutput $stdoutF -RedirectStandardError $stderrF
        $procs += $proc
        Write-Host "  Node $($n.id) started: PID=$($proc.Id)  HTTP=$($n.http)  RPC=$($n.rpc)  Data=$($n.data)"
    }
    return $procs
}

function Stop-Cluster([object[]]$procs) {
    Write-Host "Stopping 3-node cluster cleanly..."
    foreach ($p in $procs) {
        Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
    Kill-DSE
    Write-Host "Cluster stopped."
}

# ---------------------------------------------------------------------------
# 3. KAFKA PREREQUISITE VALIDATION
# ---------------------------------------------------------------------------

Write-Host "`nValidating Kafka reachability (localhost:9094)..."
$kafkaReachable = (Check-Port "localhost" 9094 3000) -or (Check-Port "127.0.0.1" 9094 3000)
if (-not $kafkaReachable) {
    Write-Error "ERROR: Kafka broker is NOT reachable on localhost:9094. Please start Kafka using:`n  docker compose -f docker/docker-compose.kafka.yml up -d`nbefore running Benchmark I."
    exit 1
}
Write-Host "  Kafka broker: reachable on localhost:9094"

$targetTopic = "documents.mutations"
$expectedPartitions = 3
$expectedRf = 1

Write-Host "Validating Kafka topic '$targetTopic' (expected partitions: $expectedPartitions, RF: $expectedRf)..."

# Step 1: Check if topic exists via kafka-topics.sh --list
$topicList = docker exec dse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>$null
$topicExists = $false
if ($topicList) {
    foreach ($t in ($topicList -split "`r?`n")) {
        if ($t.Trim() -eq $targetTopic) {
            $topicExists = $true
            break
        }
    }
}

# Step 2: If it does not exist, explicitly create it with 3 partitions and RF=1
if (-not $topicExists) {
    Write-Host "  Topic '$targetTopic' does not exist. Explicitly creating topic (--partitions $expectedPartitions, --replication-factor $expectedRf)..."
    $createOutput = docker exec dse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic $targetTopic --partitions $expectedPartitions --replication-factor $expectedRf 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Error "ERROR: Failed to create Kafka topic '$targetTopic': $createOutput"
        exit 1
    }
}

# Step 3: Inspect topic and verify PartitionCount == 3
$describeOutput = docker exec dse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic $targetTopic 2>&1
if ($LASTEXITCODE -ne 0 -or -not $describeOutput) {
    Write-Error "ERROR: Failed to describe Kafka topic '$targetTopic': $describeOutput"
    exit 1
}

$describeText = if ($describeOutput -is [array]) { $describeOutput -join "`n" } else { [string]$describeOutput }

$actualPartitions = $null
$partMatch = [regex]::Match($describeText, "PartitionCount:\s*(\d+)")
if ($partMatch.Success) {
    $actualPartitions = [int]$partMatch.Groups[1].Value
}

$actualRf = $null
$rfMatch = [regex]::Match($describeText, "ReplicationFactor:\s*(\d+)")
if ($rfMatch.Success) {
    $actualRf = [int]$rfMatch.Groups[1].Value
}

if ($actualPartitions -ne $expectedPartitions) {
    Write-Error "ERROR: Kafka topic '$targetTopic' partition mismatch! Expected PartitionCount: $expectedPartitions, but found: $(if ($null -ne $actualPartitions) { $actualPartitions } else { 'UNKNOWN' }). Benchmark I requires exactly $expectedPartitions partitions. Aborting Benchmark I before starting DSE nodes."
    exit 1
}

if ($actualRf -ne $expectedRf) {
    Write-Error "ERROR: Kafka topic '$targetTopic' replication factor mismatch! Expected ReplicationFactor: $expectedRf, but found: $(if ($null -ne $actualRf) { $actualRf } else { 'UNKNOWN' }). Benchmark I requires RF=$expectedRf. Aborting Benchmark I before starting DSE nodes."
    exit 1
}

Write-Host "  Kafka topic '$targetTopic': verified (PartitionCount: $actualPartitions, ReplicationFactor: $actualRf)"

# ---------------------------------------------------------------------------
# 4. PREPARE ISOLATED DATA DIRECTORIES & START CLUSTER
# ---------------------------------------------------------------------------

$nodes = @(
    @{ id=0; http=8081; rpc=9081; data="$DataRoot\node0" },
    @{ id=1; http=8082; rpc=9082; data="$DataRoot\node1" },
    @{ id=2; http=8083; rpc=9083; data="$DataRoot\node2" }
)

foreach ($n in $nodes) {
    New-Item -ItemType Directory -Force -Path $n.data | Out-Null
    for ($sid = 0; $sid -lt [int]$SHARDS; $sid++) {
        $shardDir = Join-Path $n.data "shard-$sid"
        New-Item -ItemType Directory -Force -Path $shardDir | Out-Null
    }
}

Kill-DSE

Write-Host "`nStarting 3-node cluster with RF=3, Shards=3, Kafka enabled..."
$clusterProcs = Start-Cluster $nodes

# Wait for HTTP health
Write-Host "Waiting for all 3 nodes to report HTTP healthy (/health)..."
foreach ($n in $nodes) {
    if (-not (Wait-Health $n.http)) {
        Write-Error "ERROR: Node $($n.id) (HTTP port $($n.http)) not healthy in time."
        Stop-Cluster $clusterProcs
        exit 1
    }
    Write-Host "  Node $($n.id) HTTP ($($n.http)): healthy"
}

# Check RPC reachability
Write-Host "Validating RPC connectivity (ports 9081, 9082, 9083)..."
foreach ($n in $nodes) {
    if (-not (Check-Port "127.0.0.1" $n.rpc)) {
        Write-Error "ERROR: Node $($n.id) RPC port $($n.rpc) is unreachable."
        Stop-Cluster $clusterProcs
        exit 1
    }
    Write-Host "  Node $($n.id) RPC ($($n.rpc)): reachable"
}

# Confirm Kafka consumer group registration
Write-Host "Confirming Kafka consumer group registration..."
$cgroupDeadline = (Get-Date).AddSeconds(25)
$groupsAssigned = $false

while ((Get-Date) -lt $cgroupDeadline) {
    try {
        $gList = docker exec dse-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list 2>$null
        if ($gList) {
            $has0 = $gList -match "dse-node-0"
            $has1 = $gList -match "dse-node-1"
            $has2 = $gList -match "dse-node-2"
            if ($has0 -and $has1 -and $has2) {
                $groupsAssigned = $true
                break
            }
        }
    } catch {
        $groupsAssigned = $true
        break
    }
    Start-Sleep -Seconds 1
}

if ($groupsAssigned) {
    Write-Host "  Kafka consumer groups (dse-node-0, dse-node-1, dse-node-2): active and assigned"
} else {
    Write-Host "  [INFO] Consumer groups will complete partition assignment during warmup phase."
}

# ---------------------------------------------------------------------------
# 5. EXECUTE BENCHMARK WORKLOAD
# ---------------------------------------------------------------------------

Write-Host "`nExecuting bench_I.py ($Warmup warmup, $Measured Kafka-down writes, $Recovery recovery writes)..."
python $BENCH_PY `
    --run-id $RunId `
    --results-dir $ResultsDir `
    --out-prefix $OutPrefix `
    --warmup $Warmup `
    --measured $Measured `
    --recovery $Recovery `
    --drain-buffer $DrainBufferSec `
    --kafka-timeout $KafkaTimeoutSec `
    --recovery-drain $RecoveryDrainSec `
    --cli-timeout $CliTimeoutSec `
    --target-port 8081

$benchExit = $LASTEXITCODE

Write-Host "bench_I.py completed with exit code: $benchExit"

# ---------------------------------------------------------------------------
# 6. CLEAN SHUTDOWN & EXIT
# ---------------------------------------------------------------------------

Stop-Cluster $clusterProcs

if ($benchExit -ne 0) {
    Write-Error "ERROR: Benchmark I failed or encountered errors/timeouts (exit code: $benchExit)."
    exit $benchExit
}

Write-Host ""
Write-Host "================================================================"
Write-Host "Benchmark I: SUCCESS"
Write-Host "  Summary CSV:  $SummaryResultPath"
Write-Host "  Writes CSV:   $WritesResultPath"
Write-Host "  Metrics CSV:  $MetricsResultPath"
Write-Host "================================================================"

exit 0
