#!/usr/bin/env pwsh
# Phase 21 Benchmark E - 3-Node Mixed Workload Orchestrator
#
# Runs mixed read/write workload across concurrency levels (C1, C2, C4, C8, C16).
# To prevent dataset growth from confounding concurrency measurements, EVERY
# concurrency level executes against its own completely isolated fresh data root:
#   data\phase21\E_mixed_3node_c1\
#   data\phase21\E_mixed_3node_c2\
#   data\phase21\E_mixed_3node_c4\
#   data\phase21\E_mixed_3node_c8\
#   data\phase21\E_mixed_3node_c16\
#
# Each concurrency level independently:
#   1. Verifies its data root does not exist or contains no benchmark data.
#   2. Starts a fresh 3-node cluster with that isolated root.
#   3. Seeds the identical 510-document baseline dataset.
#   4. Validates all 10 query terms.
#   5. Runs 50 warmup requests (35 writes, 15 searches).
#   6. Runs 500 measured mixed requests (350 writes, 150 searches).
#   7. Verifies sampled replicas on Node 0, Node 1, and Node 2.
#   8. Stops the cluster cleanly.
#
# Finally, consolidates summaries into:
#   results\phase21_E_mixed_3node_consolidated.csv

Set-StrictMode -Off
$ErrorActionPreference = "Stop"

$DSE_EXE  = "build\Debug\DistributedSearchEngine.exe"
$PSScriptRoot_Or_Benchmarks = if ($PSScriptRoot) { $PSScriptRoot } else { "benchmarks" }
$BENCH_PY = Join-Path $PSScriptRoot_Or_Benchmarks "bench_E.py"
if (-not (Test-Path $BENCH_PY)) {
    $BENCH_PY = "benchmarks\bench_E.py"
}

$RESULTS  = "results"
$PEERS    = "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083"
$SHARDS   = "3"
$RF       = "3"

$CONCURRENCY_LEVELS = @(1, 2, 4, 8, 16)

# ---------------------------------------------------------------------------
# 1. UPFRONT FRESHNESS VERIFICATION
# Verify that none of the concurrency data roots already contain benchmark data.
# NEVER delete existing data automatically.
# ---------------------------------------------------------------------------
$staleDirs = @()
foreach ($c in $CONCURRENCY_LEVELS) {
    $rootPath = "data\phase21\E_mixed_3node_c$c"
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

function Start-Cluster([string]$phaseLabel, [object[]]$nodeList) {
    $procs = @()
    foreach ($n in $nodeList) {
        $stdoutF = "$($n.data)\stdout_${phaseLabel}.log"
        $stderrF = "$($n.data)\stderr_${phaseLabel}.log"
        $args = "--node-id $($n.id) --port $($n.http) --rpc-port $($n.rpc) --peers `"$PEERS`" --shards $SHARDS --replica-factor $RF --data $($n.data)/"
        $proc = Start-Process -FilePath $DSE_EXE -ArgumentList $args -PassThru -RedirectStandardOutput $stdoutF -RedirectStandardError $stderrF
        $procs += $proc
        Write-Host "  Node $($n.id) started: PID=$($proc.Id)  HTTP=$($n.http)  RPC=$($n.rpc)  Data=$($n.data)"
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
# 2. PER-CONCURRENCY RUNS (EACH WITH ISOLATED DATA ROOT & IDENTICAL BASELINE)
# ---------------------------------------------------------------------------
$allSummaries = @()

foreach ($c in $CONCURRENCY_LEVELS) {
    $dataRoot = "data\phase21\E_mixed_3node_c$c"
    $runId    = "E_c$c"
    $outPfx   = "$RESULTS\phase21_E_mixed_3node_c$c"

    $nodes = @(
        @{ id=0; http=8081; rpc=9081; data="$dataRoot\node0" },
        @{ id=1; http=8082; rpc=9082; data="$dataRoot\node1" },
        @{ id=2; http=8083; rpc=9083; data="$dataRoot\node2" }
    )

    Write-Host ""
    Write-Host "================================================================"
    Write-Host "Benchmark E - Concurrency=$c (Isolated Root: $dataRoot)"
    Write-Host "================================================================"

    # A. Verify this concurrency data root is fresh
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

    # B. Start clean 3-node cluster for this concurrency level
    Kill-DSE
    Write-Host "Starting 3-node cluster for concurrency=$c ..."
    $procs = Start-Cluster "c$c" $nodes

    # C. Wait for HTTP health
    Write-Host "Waiting for all nodes to be healthy ..."
    foreach ($n in $nodes) {
        if (-not (Wait-Health $n.http)) {
            Write-Error "ERROR: Node $($n.id) (port $($n.http)) not healthy in time for concurrency=$c."
            Stop-Cluster $procs
            exit 1
        }
        Write-Host "  Node $($n.id) (HTTP $($n.http)) healthy."
    }

    # D. Validate RPC reachability
    Write-Host "Validating RPC reachability (ports 9081, 9082, 9083) ..."
    foreach ($n in $nodes) {
        if (-not (Check-RPC $n.rpc)) {
            Write-Error "ERROR: Node $($n.id) RPC port $($n.rpc) is UNREACHABLE for concurrency=$c. Aborting."
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
        Write-Error "ERROR: Baseline dataset preparation failed for concurrency=$c with exit code $prepExit."
        Stop-Cluster $procs
        exit $prepExit
    }
    Write-Host "Baseline dataset ready and validated for concurrency=$c."

    # F. Run mixed workload: 50 warmup + 500 measured + replica verification
    Write-Host "Running bench_E.py --concurrency $c (50 warmup + 500 measured) ..."
    python $BENCH_PY --concurrency $c --run-id $runId --out-prefix $outPfx --warmup 50 --measured 500 --target-port 8081
    $benchExit = $LASTEXITCODE
    Write-Host "bench_E.py exit=$benchExit"

    if ($benchExit -ne 0) {
        Write-Error "ERROR: bench_E.py failed with exit code $benchExit for concurrency=$c. Stopping cluster and aborting."
        Stop-Cluster $procs
        exit $benchExit
    }

    # G. Stop cluster cleanly
    Write-Host "Stopping cluster cleanly for concurrency=$c ..."
    Stop-Cluster $procs
    Write-Host "Cluster stopped."

    $allSummaries += [PSCustomObject]@{
        Concurrency = $c
        RunID       = $runId
        ExitCode    = $benchExit
        SummaryCSV  = "$outPfx`_summary.csv"
    }
}

# ---------------------------------------------------------------------------
# 3. CONSOLIDATED SUMMARY CSV
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "Writing consolidated CSV ..."
$consolidatedPath = "$RESULTS\phase21_E_mixed_3node_consolidated.csv"

$headerWritten = $false
foreach ($s in $allSummaries) {
    if (Test-Path $s.SummaryCSV) {
        $lines = Get-Content -Path $s.SummaryCSV
        if ($lines.Length -gt 0 -and -not $headerWritten) {
            $lines[0] | Out-File -Encoding utf8 -FilePath $consolidatedPath
            $headerWritten = $true
        }
        if ($lines.Length -gt 1) {
            for ($idx = 1; $idx -lt $lines.Length; $idx++) {
                if ($lines[$idx].Trim().Length -gt 0) {
                    $lines[$idx] | Add-Content -Encoding utf8 -Path $consolidatedPath
                }
            }
        }
    }
}

Write-Host ""
Write-Host "================================================================"
Write-Host "Benchmark E - ALL RUNS COMPLETE"
Write-Host "================================================================"
foreach ($s in $allSummaries) {
    Write-Host "  c=$($s.Concurrency)  exit=$($s.ExitCode)  summary=$($s.SummaryCSV)"
}
Write-Host "Consolidated summary: $consolidatedPath"
