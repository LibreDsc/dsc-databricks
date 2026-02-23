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
    if ($env:GITHUB_ACTIONS) {
        if (-not (Get-Command -Name 'Connect-AzAccount' -ErrorAction Ignore)) {
            Write-Verbose -Message "Skipping tests because Connect-AzAccount is not available in this environment."
            return
        }

        $resourceGroup = Get-AzResourceGroup | Select-Object -First 1
        $databricksInstance = Get-AzDatabricksWorkspace -ResourceGroupName $resourceGroup.ResourceGroupName | Select-Object -First 1

        if (-not $databricksInstance) {
            $params = @{
                Name = 'dbt-e2e-test-' + [Guid]::NewGuid().ToString().Substring(0, 3)
                ResourceGroupName = $resourceGroup.ResourceGroupName
                Sku = 'Premium'
                Location = $resourceGroup.Location
            }
            $databricksInstance = New-AzDatabricksWorkspace @params
        }

        $env:DATABRICKS_HOST = "https://$($databricksInstance.Url)"
        $env:DATABRICKS_TOKEN = (Get-AzAccessToken -ResourceUrl '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d').Token | ConvertFrom-SecureString -AsPlainText
    }

    $env:DSC_RESOURCE_PATH = $outputPath
    Invoke-Pester

    if ($databricksInstance) {
        Remove-AzDatabricksWorkspace -ResourceGroupName $databricksInstance.ResourceGroupName -Name $databricksInstance.Name -AsJob -NoWait
    }
}
