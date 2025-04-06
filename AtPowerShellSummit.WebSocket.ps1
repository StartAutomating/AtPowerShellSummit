#requires -Module WebSocket
param(        
[uri]
$jetstreamUrl = "wss://jetstream2.us-west.bsky.network/subscribe",

[string[]]
$Collections = @("app.bsky.feed.post"),

[string[]]
$Dids = @(),

[TimeSpan]
$Since = [TimeSpan]::FromDays(0.5),

[TimeSpan]
$TimeOut = [TimeSpan]::FromMinutes(15),

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


do {
    $Jetstream | 
        Receive-Job -ErrorAction Ignore | 
        saveMatchingMessages |
        Add-Member NoteProperty CommitMessage "Syncing from at protocol [skip ci]" -Force -PassThru
} while ($Jetstream.JobStateInfo.State -lt 3) 

$Jetstream | 
    Receive-Job -ErrorAction Ignore | 
    saveMatchingMessages |
    Add-Member NoteProperty CommitMessage "Syncing from at protocol [skip ci]" -Force -PassThru



