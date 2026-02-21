<#
.SYNOPSIS
    Builds the dsc-databricks executable and exports Microsoft Desired State Configuration (DSC) 
    manifest files.

.DESCRIPTION
    Compiles the Go project into the output/ directory and generates the 
    resources manifest file LibreDsc.Databricks.dsc.manifests.json, which contains metadata about the resources implemented in this module.
    The manifest file is used by Microsoft DSC to discover and load the resources at runtime.

.PARAMETER OutputPath
    Directory for the build artifacts. Defaults to 'output' in the repository root.

.EXAMPLE
    .\build.ps1

.EXAMPLE
    .\build.ps1 -OutputPath C:\dsc-resources\databricks
#>
[CmdletBinding()]
param (
    [Parameter()]
    [System.String]
    $OutputPath = (Join-Path $PSScriptRoot 'output'),

    [Parameter()]
    [System.String]
    $ExeName = 'dsc-databricks',

    [Parameter()]
    [switch]
    $RunTests
)

if ($IsWindows) {
    $ExeName += '.exe'
}

if (-not (Test-Path $OutputPath))
{
    New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null
}

$outputPath = Resolve-Path $OutputPath
$exePath = Join-Path $outputPath $ExeName

Write-Verbose -Message "Output path: $outputPath"
Write-Verbose -Message "Executable path: $exePath"

# Build the Go binary
Write-Verbose -Message "Building $ExeName executable..."
go build -o $exePath ./cmd
if ($LASTEXITCODE -ne 0)
{
    throw "Go build failed with exit code $LASTEXITCODE"
}

if (Test-Path $exePath)
{
    $dscManifest = & $exePath manifest | ConvertFrom-Json
    $dscManifest | ConvertTo-Json -Depth 20 | Set-Content -Path (Join-Path $outputPath 'LibreDsc.Databricks.dsc.manifests.json') -Encoding utf8
}

if ($RunTests) 
{
    $env:DSC_RESOURCE_PATH = $outputPath
    Invoke-Pester
}
