#requires -Module WebSocket
param(        
[uri]
$jetstreamUrl = "wss://jetstream$(1,2 | Get-Random).us-west.bsky.network/subscribe",

[string[]]
$Collections = @("app.bsky.feed.post"),

[string[]]
$Dids = @(),

[TimeSpan]
$Since = [TimeSpan]::FromDays(1),

[TimeSpan]
$TimeOut = [TimeSpan]::FromMinutes(20),

[Collections.IDictionary]
$AtPattern = [Ordered]@{
    "PowerShellSummit" = "\#?(?>PowerShell|PSH|Pwsh)\s{0,}Summit"
},

[string]
$Root = $PSScriptRoot,

[ValidateSet("png","jpeg")]
[string]
$ImageFormat = 'png'
)

$jetstreamUrl = @(
    "$jetstreamUrl"
    '?'
    @(
        foreach ($collection in $Collections) {            
            "wantedCollections=$([Uri]::EscapeDataString($collection))"
        }
        foreach ($did in $Dids) {
            "wantedDids=$([Uri]::EscapeDataString($did))"
        }
        "cursor=$([DateTimeOffset]::Now.Add(-$Since).ToUnixTimeMilliseconds())" 
    ) -join '&'
) -join ''

$Jetstream = WebSocket -SocketUrl $jetstreamUrl -Query @{
    wantedCollections = $collections
    cursor = ([DateTimeOffset]::Now - $since).ToUnixTimeMilliseconds()
} -TimeOut $TimeOut

filter toAtUri {
    $in = $_
    $did = $in.did
    $rkey = $in.commit.rkey
    $recordType = $in.commit.record.'$type'
    "at://$did/$recordType/$rkey"
}

filter saveImage {
    param($to)
    $in = $_
    foreach ($img in $in.commit.record.embed.images) {
        $imageRef = $img.image.ref.'$link'
        $imageLink =
            "https://cdn.bsky.app/img/feed_thumbnail/plain/$($in.did)/$imageRef@$($ImageFormat.ToLower())"
        
        $outFilePath = "$($to -replace '/$')/$imageRef.$ImageFormat"
        Invoke-WebRequest $imageLink -OutFile $outFilePath
        if ($?) {
            Get-Item -Path $outFilePath
        }        
    }
    
}

filter savePost {
    param($to)
    $in = $_
    $inAtUri = $in | toAtUri
    $inFilePath = $inAtUri -replace ':','_' -replace '^at_//', $to -replace '$', '.json'
    if (-not (Test-Path $inFilePath)) {
        New-Item -Path $inFilePath -Force -Value (ConvertTo-Json -InputObject $in -Depth 10)
        $in | saveImage "$($inFilePath | Split-Path)"
    } else {
        Get-Item -Path $inFilePath
    }
}

filter saveMatchingMessages {
    $message = $_
    foreach ($patternName in $atPattern.Keys) {
        $pattern = $atPattern[$patternName]
        if ($message.commit.record.text -match $pattern) {
            $message | savePost "$root/$($patternName)/"
        }
    }
}


Write-Host "Listening To Jetstream: $jetstreamUrl" -ForegroundColor Cyan
Write-Host "Starting loop @ $([DateTime]::Now)" -ForegroundColor Cyan
$batchStart = [DateTime]::Now
$filesFound = @()
do {
    $batch =$Jetstream | Receive-Job -ErrorAction Ignore     
    $newFiles = $batch | 
        saveMatchingMessages |
        Add-Member NoteProperty CommitMessage "Syncing from at protocol [skip ci]" -Force -PassThru
    if ($batch) {
        Write-Host "Processed batch of $($batch.Length) in $([DateTime]::Now - $batchStart) - Last Post @ $($batch[-1].commit.record.createdAt)" -ForegroundColor Green
        if ($newFiles) {
            Write-Host "Found $(@($newFiles).Length) new posts or images!" -ForegroundColor Green
            $filesFound += $newFiles
            $newFiles
        }
    }
    
    Start-Sleep -Milliseconds (Get-Random -Min .1kb -Max 1kb)
} while ($Jetstream.JobStateInfo.State -in 'NotStarted','Running') 

$Jetstream | 
    Receive-Job -ErrorAction Ignore | 
    saveMatchingMessages |
    Add-Member NoteProperty CommitMessage "Syncing from at protocol [skip ci]" -Force -PassThru



