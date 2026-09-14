#!/usr/bin/env pwsh
# Phase 21 Benchmark F - RF=1 Concurrency=1 Isolated Retry Orchestrator
#
# Dedicated runner for verifying the corrected replica placement verification
# on a single (RF=1, C=1) execution without modifying the main run_bench_F.ps1 matrix.
#
# Isolation:
#   Data root: data\phase21\F_rf1_c1_retry
#   Preserves data\phase21\F_rf1_c1 intact.

Set-StrictMode -Off
$ErrorActionPreference = "Stop"

$DSE_EXE  = "build\Debug\DistributedSearchEngine.exe"
$PSScriptRoot_Or_Benchmarks = if ($PSScriptRoot) { $PSScriptRoot } else { "benchmarks" }
$BENCH_PY = Join-Path $PSScriptRoot_Or_Benchmarks "bench_F.py"
if (-not (Test-Path $BENCH_PY)) {
    $BENCH_PY = "benchmarks\bench_F.py"
}

$RESULTS   = "results"
$PEERS     = "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083"
$SHARDS    = "3"
$RF        = 1
$C         = 1
$DATA_ROOT = "data\phase21\F_rf1_c1_retry"
$RUN_ID    = "F_rf1_c1_retry"
$OUT_PFX   = "$RESULTS\phase21_F_rf1_c1_retry"

$NODES = @(
    @{ id=0; http=8081; rpc=9081; data="$DATA_ROOT\node0" },
    @{ id=1; http=8082; rpc=9082; data="$DATA_ROOT\node1" },
    @{ id=2; http=8083; rpc=9083; data="$DATA_ROOT\node2" }
)

# ---------------------------------------------------------------------------
# 1. UPFRONT FRESHNESS VERIFICATION
# Verify data root does NOT contain benchmark data.
# NEVER delete existing data automatically.
# ---------------------------------------------------------------------------
$staleDirs = @()
foreach ($n in $NODES) {
    if (Test-Path $n.data) {
        $items = Get-ChildItem -Path $n.data -Force -ErrorAction SilentlyContinue
        if ($items -and $items.Count -gt 0) {
            $staleDirs += $n.data
        }
    }
}
if ($staleDirs.Count -gt 0) {
    Write-Error "ERROR: Data directories already contain benchmark data (not fresh): $($staleDirs -join ', '). Stop: fresh data required. Do NOT delete them automatically."
    exit 1
}

New-Item -ItemType Directory -Force -Path $RESULTS | Out-Null

function Kill-DSE {
    $procs = Get-Process -Name DistributedSearchEngine -ErrorAction SilentlyContinue
    foreach ($p in $procs) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Milliseconds 800
    $leftover = Get-Process -Name DistributedSearchEngine -ErrorAction SilentlyContinue
    if ($leftover) { Write-Error "DSE still alive after kill"; exit 1 }
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

function Check-RPC([int]$port) {
    try {
        $conn = New-Object System.Net.Sockets.TcpClient
        $result = $conn.BeginConnect("127.0.0.1", $port, $null, $null)
        $ok = $result.AsyncWaitHandle.WaitOne(1000, $false)
        $conn.Close()
        return $ok
    } catch { return $false }
}

function Start-Cluster([string]$phaseLabel, [object[]]$nodeList, [int]$rfVal) {
    $procs = @()
    foreach ($n in $nodeList) {
        $stdoutF = "$($n.data)\stdout_${phaseLabel}.log"
        $stderrF = "$($n.data)\stderr_${phaseLabel}.log"
        $args = "--node-id $($n.id) --port $($n.http) --rpc-port $($n.rpc) --peers `"$PEERS`" --shards $SHARDS --replica-factor $rfVal --data $($n.data)/"
        $proc = Start-Process -FilePath $DSE_EXE -ArgumentList $args -PassThru -RedirectStandardOutput $stdoutF -RedirectStandardError $stderrF
        $procs += $proc
        Write-Host "  Node $($n.id) started: PID=$($proc.Id)  HTTP=$($n.http)  RPC=$($n.rpc)  RF=$rfVal  Data=$($n.data)"
    }
    return $procs
}

function Stop-Cluster([object[]]$procs) {
    foreach ($p in $procs) {
        Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
    Kill-DSE
}

# ---------------------------------------------------------------------------
# 2. RUN RETRY: RF=1, CONCURRENCY=1
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "================================================================"
Write-Host "Benchmark F - RF=$RF  Concurrency=$C (Isolated Retry Root: $DATA_ROOT)"
Write-Host "================================================================"

# A. Prepare directory tree
Kill-DSE
foreach ($n in $NODES) {
    New-Item -ItemType Directory -Force -Path $n.data | Out-Null
    for ($sid = 0; $sid -lt 3; $sid++) {
        $shardDir = Join-Path $n.data "shard-$sid"
        New-Item -ItemType Directory -Force -Path $shardDir | Out-Null
    }
}

# B. Start clean 3-node cluster
Write-Host "Starting 3-node cluster for RF=$RF concurrency=$C ..."
$procs = Start-Cluster "rf1_c1_retry" $NODES $RF

# C. Wait for HTTP health
Write-Host "Waiting for all nodes to be healthy ..."
foreach ($n in $NODES) {
    if (-not (Wait-Health $n.http)) {
        Write-Error "ERROR: Node $($n.id) (port $($n.http)) not healthy in time."
        Stop-Cluster $procs
        exit 1
    }
    Write-Host "  Node $($n.id) (HTTP $($n.http)) healthy."
}

# D. Validate RPC reachability
Write-Host "Validating RPC reachability (ports 9081, 9082, 9083) ..."
foreach ($n in $NODES) {
    if (-not (Check-RPC $n.rpc)) {
        Write-Error "ERROR: Node $($n.id) RPC port $($n.rpc) is UNREACHABLE. Aborting."
        Stop-Cluster $procs
        exit 1
    }
    Write-Host "  Node $($n.id) RPC port $($n.rpc): reachable"
}

# E. Seed identical 510-document baseline dataset & validate queries
Write-Host "Seeding baseline dataset (510 documents) via Node 0 into $DATA_ROOT ..."
python $BENCH_PY --prepare --target-port 8081
$prepExit = $LASTEXITCODE
if ($prepExit -ne 0) {
    Write-Error "ERROR: Baseline dataset preparation failed with exit code $prepExit."
    Stop-Cluster $procs
    exit $prepExit
}
Write-Host "Baseline dataset ready and validated."

# F. Run benchmark: 100 warmup + 500 measured writes + replica verification
Write-Host "Running bench_F.py --replica-factor $RF --concurrency $C (100 warmup + 500 measured) ..."
python $BENCH_PY --replica-factor $RF --concurrency $C --run-id $RUN_ID --out-prefix $OUT_PFX --warmup 100 --measured 500 --target-port 8081
$benchExit = $LASTEXITCODE
Write-Host "bench_F.py exit=$benchExit"

if ($benchExit -ne 0) {
    Write-Error "ERROR: bench_F.py failed with exit code $benchExit. Stopping cluster and aborting."
    Stop-Cluster $procs
    exit $benchExit
}

# G. Stop cluster cleanly
Write-Host "Stopping cluster cleanly ..."
Stop-Cluster $procs
Write-Host "Cluster stopped. Benchmark F (RF=1, C=1) retry completed successfully."
