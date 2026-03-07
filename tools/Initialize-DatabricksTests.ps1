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

function New-TestRepoPath
{
    <#
    .SYNOPSIS
        Generates a unique workspace path for a test Git folder (repo).
    #>
    $parent = Get-DatabricksRepoParent
    return "$parent/dsc-test-repo-$(Get-Random -Minimum 10000 -Maximum 99999)"
}
