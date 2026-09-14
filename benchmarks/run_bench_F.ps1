#!/usr/bin/env pwsh
# Phase 21 Benchmark F - Replication Overhead Orchestrator
#
# Compares 3-node write performance under:
#   - RF=1 (no replication overhead, single replica per shard)
#   - RF=3 (synchronous replication to all 3 nodes per Phase 17)
#
# Concurrency levels: 1, 2, 4, 8, 16
#
# To ensure clean isolation and identical initial conditions, EVERY run
# executes against its own fresh data root:
#   data\phase21\F_official_rf1_c1\ .. data\phase21\F_official_rf1_c16\
#   data\phase21\F_official_rf3_c1\ .. data\phase21\F_official_rf3_c16\
#
# Each run:
#   1. Verifies its data root does not contain benchmark data.
#   2. Starts a fresh 3-node cluster with --replica-factor $rf.
#   3. Seeds the identical 510-document baseline dataset.
#   4. Runs 100 warmup writes (discarded).
#   5. Runs 500 measured writes (POST /documents).
#   6. Performs placement-aware replica verification on 10 sampled document IDs.
#   7. Stops cluster cleanly.
#
# Finally, consolidates summaries and computes overhead percentages into:
#   results\phase21_F_official_replication_overhead_consolidated.csv

Set-StrictMode -Off
$ErrorActionPreference = "Stop"

$DSE_EXE  = "build\Debug\DistributedSearchEngine.exe"
$PSScriptRoot_Or_Benchmarks = if ($PSScriptRoot) { $PSScriptRoot } else { "benchmarks" }
$BENCH_PY = Join-Path $PSScriptRoot_Or_Benchmarks "bench_F.py"
if (-not (Test-Path $BENCH_PY)) {
    $BENCH_PY = "benchmarks\bench_F.py"
}

$RESULTS  = "results"
$PEERS    = "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083"
$SHARDS   = "3"

$REPLICA_FACTORS    = @(1, 3)
$CONCURRENCY_LEVELS = @(1, 2, 4, 8, 16)

# ---------------------------------------------------------------------------
# 1. UPFRONT FRESHNESS VERIFICATION
# Verify that none of the 10 data roots already contain benchmark data.
# NEVER delete existing data automatically.
# ---------------------------------------------------------------------------
$staleDirs = @()
foreach ($rf in $REPLICA_FACTORS) {
    foreach ($c in $CONCURRENCY_LEVELS) {
        $rootPath = "data\phase21\F_official_rf${rf}_c${c}"
        foreach ($nodeId in @(0, 1, 2)) {
            $nodeDir = Join-Path $rootPath "node$nodeId"
            if (Test-Path $nodeDir) {
                $items = Get-ChildItem -Path $nodeDir -Force -ErrorAction SilentlyContinue
                if ($items -and $items.Count -gt 0) {
                    $staleDirs += $nodeDir
                }
            }
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

# Ensure clean initial process state
Kill-DSE

# ---------------------------------------------------------------------------
# 2. RUN MATRIX: REPLICATION FACTOR (1, 3) x CONCURRENCY (1, 2, 4, 8, 16)
# ---------------------------------------------------------------------------
$allSummaries = @()

foreach ($rf in $REPLICA_FACTORS) {
    foreach ($c in $CONCURRENCY_LEVELS) {
        $dataRoot = "data\phase21\F_official_rf${rf}_c${c}"
        $runId    = "F_official_rf${rf}_c${c}"
        $outPfx   = "$RESULTS\phase21_F_official_rf${rf}_c${c}"

        $nodes = @(
            @{ id=0; http=8081; rpc=9081; data="$dataRoot\node0" },
            @{ id=1; http=8082; rpc=9082; data="$dataRoot\node1" },
            @{ id=2; http=8083; rpc=9083; data="$dataRoot\node2" }
        )

        Write-Host ""
        Write-Host "================================================================"
        Write-Host "Benchmark F - RF=$rf  Concurrency=$c (Isolated Root: $dataRoot)"
        Write-Host "================================================================"

        # A. Verify this run's data root is fresh
        foreach ($n in $nodes) {
            if (Test-Path $n.data) {
                $items = Get-ChildItem -Path $n.data -Force -ErrorAction SilentlyContinue
                if ($items -and $items.Count -gt 0) {
                    Write-Error "ERROR: Node directory already contains benchmark data: $($n.data). Stop: fresh data required. Do NOT delete automatically."
                    exit 1
                }
            }
            New-Item -ItemType Directory -Force -Path $n.data | Out-Null
            for ($sid = 0; $sid -lt 3; $sid++) {
                $shardDir = Join-Path $n.data "shard-$sid"
                New-Item -ItemType Directory -Force -Path $shardDir | Out-Null
            }
        }

        # B. Start clean 3-node cluster
        Kill-DSE
        Write-Host "Starting 3-node cluster for RF=$rf concurrency=$c ..."
        $procs = Start-Cluster "rf${rf}_c${c}" $nodes $rf

        # C. Wait for HTTP health
        Write-Host "Waiting for all nodes to be healthy ..."
        foreach ($n in $nodes) {
            if (-not (Wait-Health $n.http)) {
                Write-Error "ERROR: Node $($n.id) (port $($n.http)) not healthy in time for RF=$rf concurrency=$c."
                Stop-Cluster $procs
                exit 1
            }
            Write-Host "  Node $($n.id) (HTTP $($n.http)) healthy."
        }

        # D. Validate RPC reachability
        Write-Host "Validating RPC reachability (ports 9081, 9082, 9083) ..."
        foreach ($n in $nodes) {
            if (-not (Check-RPC $n.rpc)) {
                Write-Error "ERROR: Node $($n.id) RPC port $($n.rpc) is UNREACHABLE for RF=$rf concurrency=$c. Aborting."
                Stop-Cluster $procs
                exit 1
            }
            Write-Host "  Node $($n.id) RPC port $($n.rpc): reachable"
        }

        # E. Seed identical 510-document baseline dataset & validate queries
        Write-Host "Seeding baseline dataset (510 documents) via Node 0 into $dataRoot ..."
        python $BENCH_PY --prepare --target-port 8081
        $prepExit = $LASTEXITCODE
        if ($prepExit -ne 0) {
            Write-Error "ERROR: Baseline dataset preparation failed for RF=$rf concurrency=$c with exit code $prepExit."
            Stop-Cluster $procs
            exit $prepExit
        }
        Write-Host "Baseline dataset ready and validated for RF=$rf concurrency=$c."

        # F. Run benchmark: 100 warmup + 500 measured writes + placement-aware replica verification
        Write-Host "Running bench_F.py --replica-factor $rf --concurrency $c (100 warmup + 500 measured) ..."
        python $BENCH_PY --replica-factor $rf --concurrency $c --run-id $runId --out-prefix $outPfx --warmup 100 --measured 500 --target-port 8081
        $benchExit = $LASTEXITCODE
        Write-Host "bench_F.py exit=$benchExit"

        if ($benchExit -ne 0) {
            Write-Error "ERROR: bench_F.py failed with exit code $benchExit for RF=$rf concurrency=$c. Stopping cluster and aborting."
            Stop-Cluster $procs
            exit $benchExit
        }

        # G. Stop cluster cleanly
        Write-Host "Stopping cluster cleanly for RF=$rf concurrency=$c ..."
        Stop-Cluster $procs
        Write-Host "Cluster stopped."

        $allSummaries += [PSCustomObject]@{
            ReplicationFactor = $rf
            Concurrency       = $c
            RunID             = $runId
            ExitCode          = $benchExit
            SummaryCSV        = "$outPfx`_summary.csv"
        }
    }
}

# ---------------------------------------------------------------------------
# 3. CONSOLIDATED REPLICATION OVERHEAD COMPARISON
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "Generating consolidated replication overhead comparison ..."
$consolidatedPath = "$RESULTS\phase21_F_official_replication_overhead_consolidated.csv"
$stagingDir = "$RESULTS\official_staging"
New-Item -ItemType Directory -Force -Path $stagingDir | Out-Null
foreach ($s in $allSummaries) {
    Copy-Item -Path $s.SummaryCSV -Destination "$stagingDir\phase21_F_rf$($s.ReplicationFactor)_c$($s.Concurrency)_summary.csv" -Force
}
python $BENCH_PY --consolidate --results-dir "$stagingDir" --out-consolidated "$consolidatedPath"
Remove-Item -Path $stagingDir -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "Consolidated comparison: $consolidatedPath"
