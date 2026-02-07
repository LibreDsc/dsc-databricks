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
