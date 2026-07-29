# Copyright (c) Gijs Reijn - All Rights Reserved
# You may use, distribute and modify this code under the
# terms of the MIT license.

function Initialize-DatabricksTests
{
    <#
    .SYNOPSIS
        Checks whether Databricks authentication environment variables are set.
    .DESCRIPTION
        Returns $true if both DATABRICKS_HOST and DATABRICKS_TOKEN are set,
        indicating that E2E tests can run against a live workspace.
    #>
    [CmdletBinding()]
    param (
        [Parameter()]
        [System.String]
        $ExeName = 'dsc-databricks'
    )

    if ($IsWindows) {
        $ExeName += '.exe'
    }

    if (-not $env:DATABRICKS_HOST)
    {
        Write-Warning 'DATABRICKS_HOST environment variable is not set. Skipping Databricks tests.'
        return $false
    }

    if (-not $env:DATABRICKS_TOKEN)
    {
        Write-Warning 'DATABRICKS_TOKEN environment variable is not set. Skipping Databricks tests.'
        return $false
    }

    if (-not (Join-Path $PSScriptRoot '..' 'output' $ExeName | Test-Path))
    {
        Write-Warning 'dsc-databricks executable not found in output directory. Run build.ps1 first. Skipping Databricks tests.'
        return $false
    }

    return $true
}

function Invoke-DscWhatIf
{
    <#
    .SYNOPSIS
        Runs dsc config set --what-if for a single resource instance.
    .DESCRIPTION
        The DSC engine only exposes what-if at config level (there is no
        'dsc resource set --what-if'), so the resource instance is wrapped in
        a minimal configuration document and piped to 'dsc config set -w'.
        Returns the parsed result document; the prediction is available at
        results[0].result.afterState and metadata.'Microsoft.DSC'.executionType
        is 'whatIf'.
    #>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [System.String]
        $ResourceType,

        [Parameter(Mandatory)]
        [System.Collections.Hashtable]
        $Properties
    )

    $config = @{
        '$schema' = 'https://aka.ms/dsc/schemas/v3/bundled/config/document.json'
        resources = @(
            @{
                name       = 'whatif'
                type       = $ResourceType
                properties = $Properties
            }
        )
    } | ConvertTo-Json -Depth 10 -Compress

    return $config | dsc config set -w -f - | ConvertFrom-Json
}

function New-TestScopeName
{
    <#
    .SYNOPSIS
        Generates a unique secret scope name for testing.
    #>
    return "dsc-test-scope-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestUserName
{
    <#
    .SYNOPSIS
        Generates a unique user name (email) for testing.
    #>
    return "dsc-test-user-$(Get-Random -Minimum 10000 -Maximum 99999)@example.com"
}

function Get-DatabricksRepoParent
{
    <#
    .SYNOPSIS
        Returns the parent path under /Repos where test repos should be created.
    .DESCRIPTION
        If DATABRICKS_REPO_PARENT is set, uses that value. Otherwise queries the
        Databricks SCIM Me endpoint to discover the current user's personal Repos
        folder (/Repos/<userName>), which is auto-created by Databricks.
    #>
    [CmdletBinding()]
    param ()

    if ($env:DATABRICKS_REPO_PARENT)
    {
        return $env:DATABRICKS_REPO_PARENT.TrimEnd('/')
    }

    if (-not $script:_repoParent)
    {
        $baseUrl = $env:DATABRICKS_HOST.TrimEnd('/')
        $headers = @{ Authorization = "Bearer $env:DATABRICKS_TOKEN" }
        $me = Invoke-RestMethod -Uri "$baseUrl/api/2.0/preview/scim/v2/Me" -Headers $headers
        $script:_repoParent = "/Repos/$($me.userName)"
        Write-Verbose "Auto-detected repo parent: $($script:_repoParent)"
    }

    return $script:_repoParent
}

function New-TestServicePrincipalName
{
    <#
    .SYNOPSIS
        Generates a unique display name for a test service principal.
    #>
    return "dsc-test-sp-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestGroupName
{
    <#
    .SYNOPSIS
        Generates a unique display name for a test group.
    #>
    return "dsc-test-group-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestClusterPolicyName
{
    <#
    .SYNOPSIS
        Generates a unique name for a test cluster policy.
    #>
    return "dsc-test-policy-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestClusterName
{
    <#
    .SYNOPSIS
        Generates a unique name for a test cluster.
    #>
    return "dsc-test-cluster-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestSqlWarehouseName
{
    <#
    .SYNOPSIS
        Generates a unique name for a test SQL warehouse.
    #>
    return "dsc-test-warehouse-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestCatalogName
{
    <#
    .SYNOPSIS
        Generates a unique name for a test Unity Catalog catalog.
    #>
    return "dsc_test_catalog_$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestSchemaName
{
    <#
    .SYNOPSIS
        Generates a unique name for a test Unity Catalog schema.
    #>
    return "dsc_test_schema_$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestVolumeName
{
    <#
    .SYNOPSIS
        Generates a unique name for a test Unity Catalog volume.
    #>
    return "dsc_test_volume_$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestStorageCredentialName
{
    <#
    .SYNOPSIS
        Generates a unique name for a test Unity Catalog storage credential.
    #>
    return "dsc-test-storagecred-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestServiceCredentialName
{
    <#
    .SYNOPSIS
        Generates a unique name for a test Unity Catalog service credential.
    #>
    return "dsc-test-servicecred-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestExternalLocationName
{
    <#
    .SYNOPSIS
        Generates a unique name for a test Unity Catalog external location.
    #>
    return "dsc-test-extloc-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestConnectionName
{
    <#
    .SYNOPSIS
        Generates a unique name for a test Unity Catalog connection.
    #>
    return "dsc-test-connection-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestRepoPath
{
    <#
    .SYNOPSIS
        Generates a unique workspace path for a test Git folder (repo).
    #>
    $parent = Get-DatabricksRepoParent
    return "$parent/dsc-test-repo-$(Get-Random -Minimum 10000 -Maximum 99999)"
}

function New-TestAccountUserName
{
    <#
    .SYNOPSIS
        Generates a unique account user name (email) for testing.
    #>
    return "dsc-test-acctuser-$(Get-Random -Minimum 10000 -Maximum 99999)@example.com"
}

function Invoke-DatabricksApi
{
    <#
    .SYNOPSIS
        Calls the Databricks REST API using DATABRICKS_HOST/DATABRICKS_TOKEN.
    .DESCRIPTION
        Internal helper for test fixture provisioning that must not depend on
        the DSC resources under test. Throws on HTTP errors.
    #>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [ValidateSet('GET', 'POST', 'PATCH', 'DELETE')]
        [System.String]
        $Method,

        [Parameter(Mandatory)]
        [System.String]
        $Path,

        [Parameter()]
        [System.Collections.Hashtable]
        $Body
    )

    $params = @{
        Uri     = "$($env:DATABRICKS_HOST.TrimEnd('/'))$Path"
        Method  = $Method
        Headers = @{ Authorization = "Bearer $env:DATABRICKS_TOKEN" }
    }
    if ($Body)
    {
        $params.ContentType = 'application/json'
        $params.Body = $Body | ConvertTo-Json -Depth 10 -Compress
    }

    return Invoke-RestMethod @params
}

function Test-UnityCatalogAvailable
{
    <#
    .SYNOPSIS
        Checks whether the workspace is attached to a Unity Catalog metastore.
    .DESCRIPTION
        Step 1 of the Unity Catalog setup guide: a workspace is UC-enabled
        when a metastore is attached. Attaching one requires an account admin
        and cannot be automated with a workspace token, so Unity Catalog test
        suites skip (not fail) when this returns $false. The result is cached
        for the lifetime of the (dot-sourced) script scope.
    #>
    [CmdletBinding()]
    param ()

    if ($null -ne $script:_unityCatalogAvailable)
    {
        return $script:_unityCatalogAvailable
    }

    try
    {
        $summary = Invoke-DatabricksApi -Method GET -Path '/api/2.1/unity-catalog/metastore_summary'
        $script:_metastoreSummary = $summary
        $script:_unityCatalogAvailable = [bool]$summary.metastore_id
    }
    catch
    {
        $script:_unityCatalogAvailable = $false
    }

    if (-not $script:_unityCatalogAvailable)
    {
        Write-Warning 'Workspace is not attached to a Unity Catalog metastore. Skipping Unity Catalog tests.'
    }

    return $script:_unityCatalogAvailable
}

function Test-UnityCatalogManagedStorageAvailable
{
    <#
    .SYNOPSIS
        Checks whether Unity Catalog fixtures with managed storage can be
        provisioned.
    .DESCRIPTION
        Managed catalogs (and therefore schemas and managed volumes) need a
        storage root: either the metastore has default managed storage, or
        DATABRICKS_CATALOG_STORAGE_LOCATION plus
        DATABRICKS_ACCESS_CONNECTOR_ID are set so the storage fixture chain
        (storage credential -> external location -> catalog storage_root)
        can be provisioned. Discovery gate for the Schema, Volume, and Grant
        suites.
    #>
    [CmdletBinding()]
    param ()

    if (-not (Test-UnityCatalogAvailable))
    {
        return $false
    }

    if ($script:_metastoreSummary -and $script:_metastoreSummary.storage_root)
    {
        return $true
    }

    if ($env:DATABRICKS_CATALOG_STORAGE_LOCATION -and $env:DATABRICKS_ACCESS_CONNECTOR_ID)
    {
        return $true
    }

    Write-Warning 'Metastore has no default managed storage and DATABRICKS_CATALOG_STORAGE_LOCATION/DATABRICKS_ACCESS_CONNECTOR_ID are not set. Skipping Unity Catalog managed storage tests.'
    return $false
}

function Initialize-UnityCatalogStorage
{
    <#
    .SYNOPSIS
        Provisions the persistent Unity Catalog storage fixtures backing
        catalog storage roots.
    .DESCRIPTION
        Idempotently creates a storage credential (dsc-ci-storage-credential,
        backed by the access connector in DATABRICKS_ACCESS_CONNECTOR_ID) and
        an external location (dsc-ci-managed-location, covering
        DATABRICKS_CATALOG_STORAGE_LOCATION) via the Unity Catalog REST API.
        Both are metastore-scoped and deliberately persistent: they survive
        ephemeral CI workspaces and are never deleted by test suites.
        Returns $true when the fixtures exist or were created.
    #>
    [CmdletBinding()]
    param ()

    if (-not ($env:DATABRICKS_CATALOG_STORAGE_LOCATION -and $env:DATABRICKS_ACCESS_CONNECTOR_ID))
    {
        return $false
    }
    if ($script:_ucStorageInitialized)
    {
        return $true
    }

    $credentialName = 'dsc-ci-storage-credential'
    $locationName = 'dsc-ci-managed-location'

    try
    {
        try
        {
            Invoke-DatabricksApi -Method GET -Path "/api/2.1/unity-catalog/storage-credentials/$credentialName" | Out-Null
        }
        catch
        {
            Invoke-DatabricksApi -Method POST -Path '/api/2.1/unity-catalog/storage-credentials' -Body @{
                name                   = $credentialName
                comment                = 'Persistent DSC CI fixture (do not delete)'
                azure_managed_identity = @{ access_connector_id = $env:DATABRICKS_ACCESS_CONNECTOR_ID }
                skip_validation        = $true
            } | Out-Null
        }

        try
        {
            Invoke-DatabricksApi -Method GET -Path "/api/2.1/unity-catalog/external-locations/$locationName" | Out-Null
        }
        catch
        {
            Invoke-DatabricksApi -Method POST -Path '/api/2.1/unity-catalog/external-locations' -Body @{
                name            = $locationName
                url             = $env:DATABRICKS_CATALOG_STORAGE_LOCATION
                credential_name = $credentialName
                comment         = 'Persistent DSC CI fixture (do not delete)'
                skip_validation = $true
            } | Out-Null
        }

        $script:_ucStorageInitialized = $true
        return $true
    }
    catch
    {
        Write-Warning "Failed to provision Unity Catalog storage fixtures: $_"
        return $false
    }
}

function Get-DatabricksCurrentUserName
{
    <#
    .SYNOPSIS
        Returns the user name (email) of the authenticated principal.
    .DESCRIPTION
        Queries the SCIM Me endpoint; works for both PAT and AAD-token
        authentication. Used by the Grant suite as a principal that is
        guaranteed to exist in the workspace.
    #>
    [CmdletBinding()]
    param ()

    if (-not $script:_currentUserName)
    {
        $me = Invoke-DatabricksApi -Method GET -Path '/api/2.0/preview/scim/v2/Me'
        $script:_currentUserName = $me.userName
    }

    return $script:_currentUserName
}

function New-UnityCatalogTestEnvironment
{
    <#
    .SYNOPSIS
        Tears up the shared Unity Catalog fixture (catalog + schema) for a suite.
    .DESCRIPTION
        Creates a uniquely named catalog with one schema inside it via the
        Unity Catalog REST API, for Unity Catalog object suites (Schema,
        Volume, ...) to use as their parent fixture. Call from BeforeAll and
        pass the returned object to Remove-UnityCatalogTestEnvironment in
        AfterAll.

        Managed data goes to the metastore's default managed storage. If the
        metastore has none, set DATABRICKS_CATALOG_STORAGE_LOCATION; a
        per-catalog subdirectory is appended because managed storage
        locations must not overlap between catalogs.

        Returns $null when the environment cannot be provisioned (callers
        should skip their tests).
    #>
    [CmdletBinding()]
    param ()

    if (-not (Test-UnityCatalogAvailable))
    {
        return $null
    }

    $catalogName = New-TestCatalogName
    $schemaName = New-TestSchemaName
    $comment = 'DSC E2E test environment (safe to delete)'

    $catalogBody = @{
        name    = $catalogName
        comment = $comment
    }
    if ($env:DATABRICKS_CATALOG_STORAGE_LOCATION)
    {
        # A catalog storage_root must be covered by an external location;
        # provision the persistent storage credential + external location
        # fixtures first.
        if (-not (Initialize-UnityCatalogStorage))
        {
            Write-Warning 'Unity Catalog storage fixtures unavailable; cannot provision a catalog with storage_root.'
            return $null
        }
        $catalogBody.storage_root = "$($env:DATABRICKS_CATALOG_STORAGE_LOCATION.TrimEnd('/'))/$catalogName"
    }

    try
    {
        Invoke-DatabricksApi -Method POST -Path '/api/2.1/unity-catalog/catalogs' -Body $catalogBody | Out-Null
        Invoke-DatabricksApi -Method POST -Path '/api/2.1/unity-catalog/schemas' -Body @{
            name         = $schemaName
            catalog_name = $catalogName
            comment      = $comment
        } | Out-Null
    }
    catch
    {
        Write-Warning "Failed to provision Unity Catalog test environment: $_"
        Remove-UnityCatalogTestEnvironment -Environment ([pscustomobject]@{ CatalogName = $catalogName })
        return $null
    }

    return [pscustomobject]@{
        CatalogName    = $catalogName
        SchemaName     = $schemaName
        SchemaFullName = "$catalogName.$schemaName"
    }
}

function Remove-UnityCatalogTestEnvironment
{
    <#
    .SYNOPSIS
        Tears down a Unity Catalog fixture created by New-UnityCatalogTestEnvironment.
    .DESCRIPTION
        Force-deletes the fixture catalog, cascading to every schema, table,
        and volume the suite created inside it. Safe to call with $null and
        never throws, so AfterAll cleanup cannot mask test results.
    #>
    [CmdletBinding()]
    param (
        [Parameter()]
        [System.Object]
        $Environment
    )

    if (-not $Environment -or -not $Environment.CatalogName)
    {
        return
    }

    try
    {
        Invoke-DatabricksApi -Method DELETE -Path "/api/2.1/unity-catalog/catalogs/$($Environment.CatalogName)?force=true" | Out-Null
    }
    catch
    {
        Write-Warning "Failed to remove Unity Catalog test environment catalog '$($Environment.CatalogName)': $_"
    }
}
