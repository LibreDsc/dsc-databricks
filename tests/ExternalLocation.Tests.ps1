[CmdletBinding()]
param (
    [Parameter()]
    [System.String]
    $ExeName = 'dsc-databricks'
)

BeforeDiscovery {
    . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
    $script:databricksAvailable = Initialize-DatabricksTests -ExeName $ExeName
    $script:externalLocationTestable = $script:databricksAvailable -and
        (Test-UnityCatalogAvailable) -and
        [bool]$env:DATABRICKS_ACCESS_CONNECTOR_ID -and
        [bool]$env:DATABRICKS_EXTERNAL_LOCATION_URL
    if ($script:databricksAvailable -and -not ($env:DATABRICKS_ACCESS_CONNECTOR_ID -and $env:DATABRICKS_EXTERNAL_LOCATION_URL)) {
        Write-Warning 'DATABRICKS_ACCESS_CONNECTOR_ID and/or DATABRICKS_EXTERNAL_LOCATION_URL are not set. Skipping ExternalLocation tests.'
    }
}

Describe 'Databricks ExternalLocation Resource' -Tag 'Databricks', 'ExternalLocation', 'UnityCatalog' -Skip:(!$script:externalLocationTestable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        # Dedicated storage credential fixture created via the raw REST API
        # (never via the resource under test).
        $script:fixtureCredentialName = New-TestStorageCredentialName
        try
        {
            Invoke-DatabricksApi -Method POST -Path '/api/2.1/unity-catalog/storage-credentials' -Body @{
                name                   = $script:fixtureCredentialName
                comment                = 'DSC E2E ExternalLocation fixture (safe to delete)'
                azure_managed_identity = @{ access_connector_id = $env:DATABRICKS_ACCESS_CONNECTOR_ID }
                skip_validation        = $true
            } | Out-Null
            $script:fixtureCredentialCreated = $true
        }
        catch
        {
            Write-Warning "Failed to provision the storage credential fixture: $_"
        }

        $script:testLocationName = New-TestExternalLocationName
        # Unique subpath in the uncovered container: Unity Catalog rejects
        # external locations that overlap an existing one.
        $script:testLocationUrl = "$($env:DATABRICKS_EXTERNAL_LOCATION_URL.TrimEnd('/'))/$($script:testLocationName)"
    }

    AfterAll {
        try
        {
            $inputJson = @{ name = $script:testLocationName } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/ExternalLocation --input $inputJson 2>$null | Out-Null
        }
        catch { }
        if ($script:fixtureCredentialCreated)
        {
            try
            {
                Invoke-DatabricksApi -Method DELETE -Path "/api/2.1/unity-catalog/storage-credentials/$($script:fixtureCredentialName)?force=true" | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/ExternalLocation | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/ExternalLocation'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/ExternalLocation | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
            $result.capabilities | Should -Contain 'setWhatIf'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/ExternalLocation | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.name | Should -Not -BeNullOrEmpty
            $result.properties.url | Should -Not -BeNullOrEmpty
            $result.properties.credential_name | Should -Not -BeNullOrEmpty
            $result.properties.read_only | Should -Not -BeNullOrEmpty
            $result.properties.fallback | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/ExternalLocation | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should require only name' {
            $result = dsc resource schema -r LibreDsc.Databricks/ExternalLocation | ConvertFrom-Json
            $result.required | Should -Be @('name')
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent external location' {
            $inputJson = @{ name = 'dsc-nonexistent-extloc-000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ExternalLocation --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set', 'Create' {
        BeforeEach {
            if (-not $script:fixtureCredentialCreated) { Set-ItResult -Skipped -Because 'the storage credential fixture was not created' }
        }

        It 'should create a new external location' {
            $inputJson = @{
                name            = $script:testLocationName
                url             = $script:testLocationUrl
                credential_name = $script:fixtureCredentialName
                comment         = 'Created by DSC test'
                skip_validation = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ExternalLocation --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:testLocationName
            $result.afterState.url | Should -Be $script:testLocationUrl
            $result.afterState.credential_name | Should -Be $script:fixtureCredentialName
            $result.changedProperties | Should -Contain 'name'
            $script:locationCreated = $true
        }

        It 'should verify the created location via get' {
            if (-not $script:locationCreated) { Set-ItResult -Skipped -Because 'the external location fixture was not created' }
            $inputJson = @{ name = $script:testLocationName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ExternalLocation --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.comment | Should -Be 'Created by DSC test'
            $result.actualState.credential_id | Should -Not -BeNullOrEmpty
            $result.actualState.owner | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Set Operation - Update' -Tag 'Set', 'Update' {
        BeforeEach {
            if (-not $script:locationCreated) { Set-ItResult -Skipped -Because 'the external location fixture was not created' }
        }

        It 'should update the comment and read_only flag' {
            $inputJson = @{
                name            = $script:testLocationName
                url             = $script:testLocationUrl
                credential_name = $script:fixtureCredentialName
                comment         = 'Updated by DSC test'
                read_only       = $true
                skip_validation = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ExternalLocation --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState.comment | Should -Be 'Updated by DSC test'
            $result.afterState.read_only | Should -Be $true
            $result.changedProperties | Should -Contain 'comment'
        }

        It 'should force-send read_only back to false' {
            $inputJson = @{
                name            = $script:testLocationName
                url             = $script:testLocationUrl
                credential_name = $script:fixtureCredentialName
                comment         = 'Updated by DSC test'
                read_only       = $false
                skip_validation = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ExternalLocation --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState.read_only | Should -Be $false
            $result.changedProperties | Should -Contain 'read_only'
        }
    }

    Context 'WhatIf Operation' -Tag 'WhatIf' {
        BeforeEach {
            if (-not $script:fixtureCredentialCreated) { Set-ItResult -Skipped -Because 'the storage credential fixture was not created' }
        }

        It 'should predict location creation without creating anything' {
            $script:whatIfLocationName = New-TestExternalLocationName
            $result = Invoke-DscWhatIf -ResourceType 'LibreDsc.Databricks/ExternalLocation' -Properties @{
                name            = $script:whatIfLocationName
                url             = "$($env:DATABRICKS_EXTERNAL_LOCATION_URL.TrimEnd('/'))/$($script:whatIfLocationName)"
                credential_name = $script:fixtureCredentialName
                comment         = 'whatif prediction'
            }
            $LASTEXITCODE | Should -Be 0
            $result.metadata.'Microsoft.DSC'.executionType | Should -Be 'whatIf'
            $result.results[0].result.afterState._exist | Should -Be $true
            $result.results[0].result.afterState.comment | Should -Be 'whatif prediction'
        }

        It 'should not have created the location' {
            $inputJson = @{ name = $script:whatIfLocationName } | ConvertTo-Json -Compress
            $get = dsc resource get -r LibreDsc.Databricks/ExternalLocation --input $inputJson | ConvertFrom-Json
            $get.actualState._exist | Should -Be $false
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        BeforeEach {
            if (-not $script:locationCreated) { Set-ItResult -Skipped -Because 'the external location fixture was not created' }
        }

        It 'should export external locations including the test location' {
            $result = dsc resource export -r LibreDsc.Databricks/ExternalLocation | ConvertFrom-Json
            $result.resources | Should -Not -BeNullOrEmpty

            $l = $result.resources | Where-Object { $_.properties.name -eq $script:testLocationName }
            $l | Should -Not -BeNullOrEmpty
            $l.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        BeforeEach {
            if (-not $script:locationCreated) { Set-ItResult -Skipped -Because 'the external location fixture was not created' }
        }

        It 'should delete the test location' {
            $inputJson = @{ name = $script:testLocationName } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/ExternalLocation --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the location is gone via get' {
            $inputJson = @{ name = $script:testLocationName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ExternalLocation --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            if (-not $script:fixtureCredentialCreated) { return }
            $script:idempotentLocationName = New-TestExternalLocationName
            $script:idempotentLocationUrl = "$($env:DATABRICKS_EXTERNAL_LOCATION_URL.TrimEnd('/'))/$($script:idempotentLocationName)"
            $inputJson = @{
                name            = $script:idempotentLocationName
                url             = $script:idempotentLocationUrl
                credential_name = $script:fixtureCredentialName
                comment         = 'Idempotency test location'
                skip_validation = $true
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/ExternalLocation --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentLocationName)
            {
                try
                {
                    $inputJson = @{ name = $script:idempotentLocationName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/ExternalLocation --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            if (-not $script:idempotentLocationName) { Set-ItResult -Skipped -Because 'the idempotency location fixture was not created' }
            $inputJson = @{
                name            = $script:idempotentLocationName
                url             = $script:idempotentLocationUrl
                credential_name = $script:fixtureCredentialName
                comment         = 'Idempotency test location'
                skip_validation = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ExternalLocation --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:idempotentLocationName
        }
    }
}
