#!/usr/bin/env pwsh
# Phase 27 Benchmark L - Sustained Moderate Load Soak Benchmark Orchestrator
#
# Launches a 3-node cluster (N=3, S=3, R=3) with max_concurrent_requests=64,
# runs benchmarks/bench_L_soak.py for the specified duration (default: 600s / 10 min),
# and cleanly stops the cluster.

param (
    [int]$DurationSec = 600
)

Set-StrictMode -Off
$ErrorActionPreference = "Stop"

$DSE_EXE  = "build\Debug\DistributedSearchEngine.exe"
$PSScriptRoot_Or_Benchmarks = if ($PSScriptRoot) { $PSScriptRoot } else { "benchmarks" }
$BENCH_PY = Join-Path $PSScriptRoot_Or_Benchmarks "bench_L_soak.py"
if (-not (Test-Path $BENCH_PY)) {
    $BENCH_PY = "benchmarks\bench_L_soak.py"
}

$RESULTS  = "results"
$PEERS    = "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083"
$SHARDS   = "3"
$RF       = "3"
$MAX_CONCURRENT = "64"

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

Kill-DSE

$dataRoot = "data\phase27\soak"
New-Item -ItemType Directory -Force -Path $dataRoot | Out-Null

$nodeList = @(
    @{ id = 0; http = 8081; rpc = 9081; data = "$dataRoot\node0" },
    @{ id = 1; http = 8082; rpc = 9082; data = "$dataRoot\node1" },
    @{ id = 2; http = 8083; rpc = 9083; data = "$dataRoot\node2" }
)

foreach ($n in $nodeList) {
    New-Item -ItemType Directory -Force -Path $n.data | Out-Null
}

$procs = @()
try {
    Write-Host "Starting 3-node cluster for Benchmark L (max_concurrent=$MAX_CONCURRENT, duration=$DurationSec s)..."
    foreach ($n in $nodeList) {
        $stdoutF = "$($n.data)\stdout.log"
        $stderrF = "$($n.data)\stderr.log"
        $args = "--node-id $($n.id) --port $($n.http) --rpc-port $($n.rpc) --peers `"$PEERS`" --shards $SHARDS --replica-factor $RF --data $($n.data)/ --max-concurrent $MAX_CONCURRENT"
        $proc = Start-Process -FilePath $DSE_EXE -ArgumentList $args -PassThru -RedirectStandardOutput $stdoutF -RedirectStandardError $stderrF
        $procs += $proc
        Write-Host "  Node $($n.id) PID=$($proc.Id) HTTP=$($n.http) RPC=$($n.rpc)"
    }

    Write-Host "Waiting for Node 0 health..."
    if (-not (Wait-Health 8081)) {
        Write-Error "Node 0 failed to report healthy."
        exit 1
    }
    Write-Host "Cluster healthy. Running Benchmark L..."

    python $BENCH_PY --port 8081 --rpc-ports 9081 9082 9083 --concurrency 32 --duration $DurationSec --sample-interval 10 --output-dir $RESULTS
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Benchmark L failed with exit code $LASTEXITCODE"
    }
} finally {
    Write-Host "Stopping cluster..."
    foreach ($p in $procs) {
        Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
    Kill-DSE
    Write-Host "Cluster stopped."
}
