$ErrorActionPreference = 'Stop'

$DefaultRepo = 'stamler/sympath'

function Fail([string]$Message) {
    throw $Message
}

# SYMPATH_INSTALL_BASE_URL and SYMPATH_INSTALL_BASE_DIR are internal
# test hooks. Public install overrides are documented in README.md.
function Get-ReleaseUrl([string]$AssetName) {
    if ($env:SYMPATH_INSTALL_BASE_DIR) {
        return (Join-Path $env:SYMPATH_INSTALL_BASE_DIR $AssetName)
    }

    if ($env:SYMPATH_INSTALL_BASE_URL) {
        return ('{0}/{1}' -f $env:SYMPATH_INSTALL_BASE_URL.TrimEnd('/'), $AssetName)
    }

    $repo = if ($env:SYMPATH_INSTALL_REPO) { $env:SYMPATH_INSTALL_REPO } else { $DefaultRepo }
    if ($env:SYMPATH_INSTALL_VERSION) {
        return ('https://github.com/{0}/releases/download/{1}/{2}' -f $repo, $env:SYMPATH_INSTALL_VERSION, $AssetName)
    }

    return ('https://github.com/{0}/releases/latest/download/{1}' -f $repo, $AssetName)
}

function Get-AssetName() {
    if ($PSVersionTable.PSEdition -and $PSVersionTable.Platform -and $PSVersionTable.Platform -ne 'Win32NT') {
        Fail "unsupported platform $($PSVersionTable.Platform); the installer currently supports Windows amd64 only"
    }

    $arch = if ($env:PROCESSOR_ARCHITEW6432) {
        $env:PROCESSOR_ARCHITEW6432
    } else {
        $env:PROCESSOR_ARCHITECTURE
    }

    if ($arch -ne 'AMD64') {
        Fail "unsupported platform Windows/$arch; the installer currently supports Windows amd64 only"
    }

    return 'sympath-windows-amd64.zip'
}

function Get-ExpectedChecksum([string]$ChecksumsPath, [string]$AssetName) {
    foreach ($line in Get-Content -Path $ChecksumsPath) {
        $parts = $line -split '\s+'
        if ($parts.Length -ge 2 -and $parts[1] -eq $AssetName) {
            return $parts[0]
        }
    }

    Fail "no checksum found for $AssetName"
}

function Get-SHA256Hex([string]$Path) {
    $stream = [System.IO.File]::OpenRead($Path)
    try {
        $sha256 = [System.Security.Cryptography.SHA256]::Create()
        try {
            $hashBytes = $sha256.ComputeHash($stream)
        } finally {
            $sha256.Dispose()
        }
    } finally {
        $stream.Dispose()
    }

    return ([System.BitConverter]::ToString($hashBytes)).Replace('-', '').ToLowerInvariant()
}

function Prepend-PathEntry([string]$PathValue, [string]$Entry) {
    $parts = New-Object System.Collections.Generic.List[string]
    foreach ($part in ($PathValue -split ';')) {
        if ([string]::IsNullOrWhiteSpace($part)) {
            continue
        }
        if ($part.TrimEnd('\') -ieq $Entry.TrimEnd('\')) {
            continue
        }
        $parts.Add($part)
    }

    if ($parts.Count -eq 0) {
        return $Entry
    }
    return ('{0};{1}' -f $Entry, ($parts -join ';'))
}

function Ensure-UserPathEntry([string]$InstallDir) {
    $userPath = [Environment]::GetEnvironmentVariable('PATH', 'User')
    $updatedUserPath = Prepend-PathEntry $userPath $InstallDir
    if ($updatedUserPath -ne $userPath) {
        [Environment]::SetEnvironmentVariable('PATH', $updatedUserPath, 'User')
    }

    $updatedSessionPath = Prepend-PathEntry $env:PATH $InstallDir
    if ($updatedSessionPath -ne $env:PATH) {
        $env:PATH = $updatedSessionPath
    }
}

function Fetch-Asset([string]$AssetName, [string]$DestinationPath) {
    $source = Get-ReleaseUrl $AssetName
    if ($env:SYMPATH_INSTALL_BASE_DIR) {
        Copy-Item -Path $source -Destination $DestinationPath -Force
        return
    }

    Invoke-WebRequest -Uri $source -OutFile $DestinationPath
}

function Main() {
    $assetName = Get-AssetName
    $localAppData = if ($env:LOCALAPPDATA) { $env:LOCALAPPDATA } else { Join-Path $HOME 'AppData\Local' }
    $installDir = if ($env:SYMPATH_INSTALL_DIR) { $env:SYMPATH_INSTALL_DIR } else { Join-Path $localAppData 'Programs\sympath\bin' }
    $targetPath = Join-Path $installDir 'sympath.exe'
    $tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('sympath-install-' + [System.Guid]::NewGuid().ToString('N'))
    $archivePath = Join-Path $tempRoot $assetName
    $checksumsPath = Join-Path $tempRoot 'checksums.txt'
    $extractDir = Join-Path $tempRoot 'extracted'
    $downloadedBinary = Join-Path $extractDir 'sympath.exe'

    New-Item -ItemType Directory -Force -Path $installDir, $extractDir | Out-Null

    try {
        Write-Host "Downloading $assetName"
        Fetch-Asset -AssetName $assetName -DestinationPath $archivePath
        Fetch-Asset -AssetName 'checksums.txt' -DestinationPath $checksumsPath

        $expected = Get-ExpectedChecksum -ChecksumsPath $checksumsPath -AssetName $assetName
        $actual = Get-SHA256Hex -Path $archivePath
        if ($actual -ne $expected.ToLowerInvariant()) {
            Fail "checksum verification failed for $assetName"
        }

        Expand-Archive -Path $archivePath -DestinationPath $extractDir -Force
        if (-not (Test-Path -Path $downloadedBinary -PathType Leaf)) {
            Fail 'downloaded archive did not contain a runnable sympath.exe binary'
        }

        $downloadedVersion = (& $downloadedBinary version).Trim()
        $installedVersion = $null
        if (Test-Path -Path $targetPath -PathType Leaf) {
            try {
                $installedVersion = (& $targetPath version 2>$null).Trim()
            } catch {
                $installedVersion = $null
            }
        }

        if ($installedVersion -and $installedVersion -eq $downloadedVersion) {
            Write-Host "sympath $installedVersion is already installed at $targetPath"
        } else {
            Copy-Item -Path $downloadedBinary -Destination $targetPath -Force
            Write-Host "Installed sympath $downloadedVersion to $targetPath"
        }

        Ensure-UserPathEntry -InstallDir $installDir
        $resolved = (Get-Command sympath -ErrorAction Stop).Source
        Write-Host "sympath is available now: $resolved"
    } finally {
        if (Test-Path -Path $tempRoot) {
            Remove-Item -Recurse -Force $tempRoot
        }
    }
}

Main
