<#
.SYNOPSIS
    Builds the dsc-databricks executable and exports Microsoft Desired State Configuration (DSC) 
    manifest files.

.DESCRIPTION
    Compiles the Go project into the output/ directory and generates one
    <type>.dsc.resource.json manifest file per resource, which contains metadata about the resources implemented in this module.
    The manifest files are used by Microsoft DSC to discover and load the resources at runtime.

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
    Get-ChildItem -Path $outputPath -Filter '*.dsc.resource.json' -ErrorAction Ignore | Remove-Item
    # Remove the pre-migration aggregate manifest if it is still around.
    Remove-Item -Path (Join-Path $outputPath 'LibreDsc.Databricks.dsc.manifests.json') -ErrorAction Ignore
    & $exePath manifest --out-dir $outputPath
    if ($LASTEXITCODE -ne 0)
    {
        throw "Manifest generation failed with exit code $LASTEXITCODE"
    }
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

        # --- Unity Catalog storage bootstrap -------------------------------
        # Persistent, reuse-if-exists infrastructure for the UC storage
        # suites: an ADLS Gen2 storage account with two containers ('managed'
        # for catalog storage roots, 'external' for the ExternalLocation
        # suite's own locations), a Databricks Access Connector, and a
        # Storage Blob Data Contributor role assignment for the connector's
        # system-assigned identity. Runs before workspace creation so RBAC
        # has the workspace provisioning + warm-up time to propagate. On any
        # failure the storage-dependent suites skip instead of failing.
        try {
            $rgName = $resourceGroup.ResourceGroupName

            # Deterministic, globally unique-ish storage account name
            # (3-24 chars, lowercase alphanumerics only).
            $hashInput = "$rgName/$((Get-AzContext).Subscription.Id)"
            $sha = [System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($hashInput))
            $suffix = ([System.Convert]::ToHexString($sha)).Substring(0, 12).ToLowerInvariant()
            $storageAccountName = "dscdbx$suffix"

            $storageAccount = Get-AzStorageAccount -ResourceGroupName $rgName -Name $storageAccountName -ErrorAction Ignore
            if (-not $storageAccount) {
                Write-Verbose -Message "Creating ADLS Gen2 storage account $storageAccountName..."
                $storageAccount = New-AzStorageAccount -ResourceGroupName $rgName -Name $storageAccountName `
                    -Location $resourceGroup.Location -SkuName Standard_LRS -Kind StorageV2 `
                    -EnableHierarchicalNamespace $true -MinimumTlsVersion TLS1_2
            }
            foreach ($containerName in 'managed', 'external') {
                if (-not (Get-AzStorageContainer -Name $containerName -Context $storageAccount.Context -ErrorAction Ignore)) {
                    New-AzStorageContainer -Name $containerName -Context $storageAccount.Context | Out-Null
                }
            }

            $connector = Get-AzDatabricksAccessConnector -ResourceGroupName $rgName -Name 'dsc-ci-access-connector' -ErrorAction Ignore
            if (-not $connector) {
                Write-Verbose -Message 'Creating Databricks access connector dsc-ci-access-connector...'
                $connector = New-AzDatabricksAccessConnector -ResourceGroupName $rgName -Name 'dsc-ci-access-connector' `
                    -Location $resourceGroup.Location -IdentityType 'SystemAssigned'
            }
            $env:DATABRICKS_ACCESS_CONNECTOR_ID = $connector.Id

            $principalId = $connector.Identity.PrincipalId ?? $connector.IdentityPrincipalId
            if (-not $principalId) {
                Start-Sleep -Seconds 15
                $connector = Get-AzDatabricksAccessConnector -ResourceGroupName $rgName -Name 'dsc-ci-access-connector'
                $principalId = $connector.Identity.PrincipalId ?? $connector.IdentityPrincipalId
            }
            if (-not $principalId) {
                throw "Access connector managed identity principal id could not be resolved from $($connector | ConvertTo-Json -Depth 3 -Compress)"
            }
            $assigned = Get-AzRoleAssignment -ObjectId $principalId -Scope $storageAccount.Id -ErrorAction Ignore |
                Where-Object RoleDefinitionName -eq 'Storage Blob Data Contributor'
            if (-not $assigned) {
                Write-Verbose -Message 'Assigning Storage Blob Data Contributor to the access connector identity...'
                New-AzRoleAssignment -ObjectId $principalId -RoleDefinitionName 'Storage Blob Data Contributor' -Scope $storageAccount.Id | Out-Null
            }

            $env:DATABRICKS_CATALOG_STORAGE_LOCATION = "abfss://managed@$storageAccountName.dfs.core.windows.net/catalogs"
            $env:DATABRICKS_EXTERNAL_LOCATION_URL = "abfss://external@$storageAccountName.dfs.core.windows.net"
        }
        catch {
            Write-Warning "Unity Catalog storage bootstrap failed: $_"
            # Storage-root-dependent suites (Catalog storage contexts,
            # Schema, Volume, Grant, ExternalLocation) skip without these.
            # DATABRICKS_ACCESS_CONNECTOR_ID stays if it was set — the
            # credential suites only need the connector (skip_validation).
            $env:DATABRICKS_CATALOG_STORAGE_LOCATION = $null
            $env:DATABRICKS_EXTERNAL_LOCATION_URL = $null
        }
        # -------------------------------------------------------------------

        $workspaceCreated = $false
        if (-not $databricksInstance) {
            $params = @{
                Name = 'dbt-e2e-test-' + [Guid]::NewGuid().ToString().Substring(0, 3)
                ResourceGroupName = $resourceGroup.ResourceGroupName
                Sku = 'Premium'
                Location = $resourceGroup.Location
            }
            $databricksInstance = New-AzDatabricksWorkspace @params
            $workspaceCreated = $true
        }

        $env:DATABRICKS_HOST = "https://$($databricksInstance.Url)"
        $env:DATABRICKS_TOKEN = (Get-AzAccessToken -ResourceUrl '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d').Token | ConvertFrom-SecureString -AsPlainText

        if ($workspaceCreated) {
            Write-Verbose -Message 'Waiting for the new workspace cluster service to become ready...'
            $headers = @{ Authorization = "Bearer $env:DATABRICKS_TOKEN" }
            $deadline = [DateTime]::UtcNow.AddMinutes(10)
            do {
                try {
                    $versions = Invoke-RestMethod -Uri "$env:DATABRICKS_HOST/api/2.0/clusters/spark-versions" -Headers $headers
                    if ($versions.versions.Count -gt 0) { break }
                }
                catch { }
                Start-Sleep -Seconds 15
            } while ([DateTime]::UtcNow -lt $deadline)
            Write-Verbose -Message 'Cluster service responded; granting the workspace a warm-up period.'
            Start-Sleep -Seconds 120
        }
    }

    $env:DSC_RESOURCE_PATH = $outputPath
    $pesterConfig = New-PesterConfiguration
    $pesterConfig.Run.Path = Join-Path $PSScriptRoot 'tests'
    $pesterConfig.Run.PassThru = $true
    $failures = (Invoke-Pester -Configuration $pesterConfig -ErrorAction Ignore).FailedCount

    if ($databricksInstance -and $env:GITHUB_ACTIONS) {
        Remove-AzDatabricksWorkspace -ResourceGroupName $databricksInstance.ResourceGroupName -Name $databricksInstance.Name -AsJob -NoWait
    }

    if ($failures -ne 0) {
        throw "$failures tests failed."
    }
}
