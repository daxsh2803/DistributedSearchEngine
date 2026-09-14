#!/usr/bin/env pwsh
# Phase 21 Benchmark D - 3-Node Search Orchestrator
# Prepares 510-document dataset, starts 3-node cluster, runs bench_D.py across concurrency levels (C1, C2, C4, C8, C16),
# and generates per-concurrency and consolidated CSV summaries.

Set-StrictMode -Off
$ErrorActionPreference = "Stop"

$DSE_EXE  = "build\Debug\DistributedSearchEngine.exe"
$PSScriptRoot_Or_Benchmarks = if ($PSScriptRoot) { $PSScriptRoot } else { "benchmarks" }
$BENCH_PY = Join-Path $PSScriptRoot_Or_Benchmarks "bench_D.py"
if (-not (Test-Path $BENCH_PY)) {
    $BENCH_PY = "benchmarks\bench_D.py"
}

$RESULTS  = "results"
$PEERS    = "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083"
$SHARDS   = "3"
$RF       = "3"

$DATA_ROOT = "data\phase21\D_search_3node"

$NODES = @(
    @{ id=0; http=8081; rpc=9081; data="$DATA_ROOT\node0" },
    @{ id=1; http=8082; rpc=9082; data="$DATA_ROOT\node1" },
    @{ id=2; http=8083; rpc=9083; data="$DATA_ROOT\node2" }
)

# ---------------------------------------------------------------------------
# 1. DATA DIRECTORY ISOLATION & VERIFICATION
# Detect whether any node directory already contains benchmark data.
# Do NOT delete them automatically.
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
    Write-Error "ERROR: Benchmark data directories already contain benchmark data (not fresh): $($staleDirs -join ', '). Stop: fresh data required. Do NOT delete them automatically."
    exit 1
}

New-Item -ItemType Directory -Force -Path $RESULTS | Out-Null
foreach ($n in $NODES) {
    if (-not (Test-Path $n.data)) {
        New-Item -ItemType Directory -Force -Path $n.data | Out-Null
    }
    # Ensure shard subdirectories exist so DocumentStore::save() persists cleanly
    for ($sid = 0; $sid -lt 3; $sid++) {
        $shardDir = Join-Path $n.data "shard-$sid"
        if (-not (Test-Path $shardDir)) {
            New-Item -ItemType Directory -Force -Path $shardDir | Out-Null
        }
    }
}

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

function Start-Cluster([string]$phaseLabel) {
    $procs = @()
    foreach ($n in $NODES) {
        $stdoutF = "$($n.data)\stdout_${phaseLabel}.log"
        $stderrF = "$($n.data)\stderr_${phaseLabel}.log"
        $args = "--node-id $($n.id) --port $($n.http) --rpc-port $($n.rpc) --peers `"$PEERS`" --shards $SHARDS --replica-factor $RF --data $($n.data)/"
        $proc = Start-Process -FilePath $DSE_EXE -ArgumentList $args -PassThru -RedirectStandardOutput $stdoutF -RedirectStandardError $stderrF
        $procs += $proc
        Write-Host "  Node $($n.id) started: PID=$($proc.Id)  HTTP=$($n.http)  RPC=$($n.rpc)"
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

# Ensure clean initial state
Kill-DSE

# ---------------------------------------------------------------------------
# 2. DATASET PREPARATION PHASE
# Starts cluster, seeds 510 documents (500 M + 10 coverage) via Node 0,
# validates all 10 QUERY_TERMS, and cleanly stops the cluster to persist data.
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "================================================================"
Write-Host "Benchmark D - Preparing Dataset (510 documents)"
Write-Host "================================================================"
Write-Host "Starting cluster for dataset preparation ..."
$prepProcs = Start-Cluster "prepare"

Write-Host "Waiting for all nodes to be healthy ..."
foreach ($n in $NODES) {
    if (-not (Wait-Health $n.http)) {
        Write-Error "ERROR: Node $($n.id) (port $($n.http)) not healthy during preparation."
        Stop-Cluster $prepProcs
        exit 1
    }
    Write-Host "  Node $($n.id) (HTTP $($n.http)) healthy."
}

Write-Host "Validating RPC reachability (ports 9081, 9082, 9083) ..."
foreach ($n in $NODES) {
    if (-not (Check-RPC $n.rpc)) {
        Write-Error "ERROR: Node $($n.id) RPC port $($n.rpc) unreachable during preparation."
        Stop-Cluster $prepProcs
        exit 1
    }
    Write-Host "  Node $($n.id) RPC port $($n.rpc): reachable"
}

Write-Host "Seeding 510 documents via Node 0 (replicated to all 3 nodes) ..."
python $BENCH_PY --prepare --target-port 8081
$prepExit = $LASTEXITCODE
if ($prepExit -ne 0) {
    Write-Error "ERROR: Dataset preparation failed with exit code $prepExit."
    Stop-Cluster $prepProcs
    exit $prepExit
}

Write-Host "Dataset preparation successful. Stopping cluster to persist state ..."
Stop-Cluster $prepProcs
Write-Host "Preparation cluster stopped."

# ---------------------------------------------------------------------------
# 3. MEASURED BENCHMARK SWEEP (C1, C2, C4, C8, C16)
# ---------------------------------------------------------------------------
$allSummaries = @()

foreach ($c in @(1, 2, 4, 8, 16)) {
    $runId  = "D_c$c"
    $outPfx = "$RESULTS\phase21_D_search_3node_c$c"
    $concLabel = "c$c"

    Write-Host ""
    Write-Host "================================================================"
    Write-Host "Benchmark D - Concurrency=$c"
    Write-Host "================================================================"

    # Start all 3 nodes (loads persisted shards locally)
    Write-Host "Starting 3-node cluster ..."
    $procs = Start-Cluster $concLabel

    # Wait for all HTTP ports to be healthy
    Write-Host "Waiting for all nodes to be healthy ..."
    foreach ($n in $NODES) {
        if (-not (Wait-Health $n.http)) {
            Write-Error "ERROR: Node $($n.id) (port $($n.http)) not healthy in time."
            Stop-Cluster $procs
            exit 1
        }
        Write-Host "  Node $($n.id) (HTTP $($n.http)) healthy."
    }

    # Validate RPC reachability
    Write-Host "Validating RPC reachability (ports 9081, 9082, 9083) ..."
    foreach ($n in $NODES) {
        if (-not (Check-RPC $n.rpc)) {
            Write-Error "ERROR: Node $($n.id) RPC port $($n.rpc) is UNREACHABLE after startup. Aborting benchmark."
            Stop-Cluster $procs
            exit 1
        }
        Write-Host "  Node $($n.id) RPC port $($n.rpc): reachable"
    }

    # Run benchmark: 50 warmup + 500 measured searches against Node 0
    Write-Host "Running bench_D.py --concurrency $c ..."
    python $BENCH_PY --concurrency $c --run-id $runId --out-prefix $outPfx --warmup 50 --measured 500 --target-port 8081
    $exitCode = $LASTEXITCODE
    Write-Host "bench_D.py exit=$exitCode"

    if ($exitCode -ne 0) {
        Write-Error "ERROR: bench_D.py failed with exit code $exitCode. Stopping cluster and aborting."
        Stop-Cluster $procs
        exit $exitCode
    }

    # Stop cluster cleanly after benchmark completes
    Write-Host "Stopping cluster cleanly ..."
    Stop-Cluster $procs
    Write-Host "Cluster stopped."

    $allSummaries += [PSCustomObject]@{
        Concurrency = $c
        RunID       = $runId
        ExitCode    = $exitCode
        SummaryCSV  = "$outPfx`_summary.csv"
    }
}

# ---------------------------------------------------------------------------
# 4. CONSOLIDATED SUMMARY CSV
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "Writing consolidated CSV ..."
$consolidatedPath = "$RESULTS\phase21_D_search_3node_consolidated.csv"
$header = "run_id,concurrency,requests,successful,errors,p50_ms,p95_ms,p99_ms,mean_ms,elapsed_s,throughput_req_s,delta_coordinator_searches,delta_coordinator_success,delta_coordinator_errors,delta_searches_total,delta_search_errors"
$header | Out-File -Encoding utf8 -FilePath $consolidatedPath
foreach ($s in $allSummaries) {
    if (Test-Path $s.SummaryCSV) {
        $rows = Import-Csv $s.SummaryCSV
        foreach ($r in $rows) {
            "$($r.run_id),$($r.concurrency),$($r.requests),$($r.successful),$($r.errors),$($r.p50_ms),$($r.p95_ms),$($r.p99_ms),$($r.mean_ms),$($r.elapsed_s),$($r.throughput_req_s),$($r.delta_coordinator_searches),$($r.delta_coordinator_success),$($r.delta_coordinator_errors),$($r.delta_searches_total),$($r.delta_search_errors)" | Add-Content -Encoding utf8 -Path $consolidatedPath
        }
    }
}

Write-Host ""
Write-Host "================================================================"
Write-Host "Benchmark D - ALL RUNS COMPLETE"
Write-Host "================================================================"
foreach ($s in $allSummaries) {
    Write-Host "  c=$($s.Concurrency)  exit=$($s.ExitCode)  summary=$($s.SummaryCSV)"
}
Write-Host "Consolidated summary: $consolidatedPath"
