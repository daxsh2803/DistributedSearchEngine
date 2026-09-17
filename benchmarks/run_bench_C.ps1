#!/usr/bin/env pwsh
# Phase 21 Benchmark C — 3-Node Indexing Orchestrator
# Starts all 3 DSE nodes for EACH concurrency level, runs bench_C.py, then stops all 3.

Set-StrictMode -Off
$ErrorActionPreference = "Stop"

$DSE_EXE  = "build\Debug\DistributedSearchEngine.exe"
$PSScriptRoot_Or_Scratch = if ($PSScriptRoot) { $PSScriptRoot } else { "benchmarks" }
$BENCH_PY = Join-Path $PSScriptRoot_Or_Scratch "bench_C.py"
if (-not (Test-Path $BENCH_PY)) {
    $BENCH_PY = "benchmarks\bench_C.py"
}

$RESULTS  = "results"
$PEERS    = "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083"
$SHARDS   = "3"
$RF       = "3"

$NODES = @(
    @{ id=0; http=8081; rpc=9081; data="data\phase21\C_index_3node_run3\node0" },
    @{ id=1; http=8082; rpc=9082; data="data\phase21\C_index_3node_run3\node1" },
    @{ id=2; http=8083; rpc=9083; data="data\phase21\C_index_3node_run3\node2" }
)

# ---------------------------------------------------------------------------
# 4. DATA DIRECTORY ISOLATION
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

function Start-Cluster([string]$concLabel) {
    $procs = @()
    foreach ($n in $NODES) {
        $stdoutF = "$($n.data)\stdout_${concLabel}.log"
        $stderrF = "$($n.data)\stderr_${concLabel}.log"
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

function Check-RPC([int]$port) {
    try {
        $conn = New-Object System.Net.Sockets.TcpClient
        $result = $conn.BeginConnect("127.0.0.1", $port, $null, $null)
        $ok = $result.AsyncWaitHandle.WaitOne(1000, $false)
        $conn.Close()
        return $ok
    } catch { return $false }
}

# Ensure clean start
Kill-DSE

$allSummaries = @()

foreach ($c in @(1, 2, 4, 8, 16)) {
    $runId  = "C_c$c"
    $outPfx = "$RESULTS\phase21_C_index_3node_c$c"
    $concLabel = "c$c"

    Write-Host ""
    Write-Host "================================================================"
    Write-Host "Benchmark C - Concurrency=$c"
    Write-Host "================================================================"

    # Start all 3 nodes
    Write-Host "Starting 3-node cluster ..."
    $procs = Start-Cluster $concLabel

    # Wait for all HTTP ports to be healthy
    Write-Host "Waiting for all nodes to be healthy ..."
    foreach ($n in $NODES) {
        if (-not (Wait-Health $n.http)) {
            Write-Error "Node $($n.id) (port $($n.http)) not healthy in time."
            Stop-Cluster $procs; exit 1
        }
        Write-Host "  Node $($n.id) (HTTP $($n.http)) healthy."
    }

    # -----------------------------------------------------------------------
    # 2. RPC VALIDATION
    # If any RPC port is unreachable, stop cluster, report failure, abort.
    # -----------------------------------------------------------------------
    Write-Host "Validating RPC reachability (ports 9081, 9082, 9083) ..."
    foreach ($n in $NODES) {
        $rpcOk = Check-RPC $n.rpc
        if (-not $rpcOk) {
            Write-Error "ERROR: Node $($n.id) RPC port $($n.rpc) is UNREACHABLE after startup. Aborting benchmark."
            Stop-Cluster $procs
            exit 1
        }
        Write-Host "  Node $($n.id) RPC port $($n.rpc): reachable"
    }

    # -----------------------------------------------------------------------
    # Run benchmark (includes 500 measured requests followed by
    # deterministic sampled replica verification before cluster shutdown)
    # -----------------------------------------------------------------------
    Write-Host "Running bench_C.py --concurrency $c ..."
    python $BENCH_PY --concurrency $c --run-id $runId --out-prefix $outPfx --warmup 100 --measured 500
    $exitCode = $LASTEXITCODE
    Write-Host "bench_C.py exit=$exitCode"

    if ($exitCode -ne 0) {
        Write-Error "ERROR: bench_C.py failed with exit code $exitCode (benchmark or replica verification failure). Stopping cluster cleanly and aborting."
        Stop-Cluster $procs
        exit $exitCode
    }

    # Stop cluster cleanly after benchmark and replica verification complete
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

# Produce consolidated CSV
Write-Host ""
Write-Host "Writing consolidated CSV ..."
$header = "run_id,concurrency,requests,successful,errors,p50_ms,p95_ms,p99_ms,mean_ms,elapsed_s,throughput_req_s,delta_writes_total,delta_write_success,delta_write_errors,delta_events_total,delta_events_published,delta_events_failed"
$header | Out-File -Encoding utf8 -FilePath "$RESULTS\phase21_C_index_3node_consolidated.csv"
foreach ($s in $allSummaries) {
    if (Test-Path $s.SummaryCSV) {
        $rows = Import-Csv $s.SummaryCSV
        foreach ($r in $rows) {
            "$($r.run_id),$($r.concurrency),$($r.requests),$($r.successful),$($r.errors),$($r.p50_ms),$($r.p95_ms),$($r.p99_ms),$($r.mean_ms),$($r.elapsed_s),$($r.throughput_req_s),$($r.delta_writes_total),$($r.delta_write_success),$($r.delta_write_errors),$($r.delta_events_total),$($r.delta_events_published),$($r.delta_events_failed)" | Add-Content -Encoding utf8 -Path "$RESULTS\phase21_C_index_3node_consolidated.csv"
        }
    }
}

Write-Host ""
Write-Host "================================================================"
Write-Host "Benchmark C - ALL RUNS COMPLETE"
Write-Host "================================================================"
foreach ($s in $allSummaries) {
    Write-Host "  c=$($s.Concurrency)  exit=$($s.ExitCode)  summary=$($s.SummaryCSV)"
}
