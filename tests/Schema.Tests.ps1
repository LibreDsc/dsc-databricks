[CmdletBinding()]
param (
    [Parameter()]
    [System.String]
    $ExeName = 'dsc-databricks'
)

BeforeDiscovery {
    . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
    $script:databricksAvailable = Initialize-DatabricksTests -ExeName $ExeName
    $script:unityCatalogStorageAvailable = $script:databricksAvailable -and (Test-UnityCatalogManagedStorageAvailable)
}

Describe 'Databricks Schema Resource' -Tag 'Databricks', 'Schema', 'UnityCatalog' -Skip:(!$script:unityCatalogStorageAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:ucEnv = New-UnityCatalogTestEnvironment
        $script:testSchemaName = New-TestSchemaName
    }

    AfterAll {
        Remove-UnityCatalogTestEnvironment -Environment $script:ucEnv
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/Schema | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/Schema'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/Schema | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
            $result.capabilities | Should -Contain 'setWhatIf'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/Schema | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.name | Should -Not -BeNullOrEmpty
            $result.properties.catalog_name | Should -Not -BeNullOrEmpty
            $result.properties.comment | Should -Not -BeNullOrEmpty
            $result.properties.storage_root | Should -Not -BeNullOrEmpty
            $result.properties.enable_predictive_optimization | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/Schema | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should require name and catalog_name' {
            $result = dsc resource schema -r LibreDsc.Databricks/Schema | ConvertFrom-Json
            $result.required | Should -Contain 'name'
            $result.required | Should -Contain 'catalog_name'
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent schema' {
            if (-not $script:ucEnv) { Set-ItResult -Skipped -Because 'the Unity Catalog fixture was not provisioned' }
            $inputJson = @{ name = 'dsc_nonexistent_schema_000'; catalog_name = $script:ucEnv.CatalogName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Schema --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set', 'Create' {
        BeforeEach {
            if (-not $script:ucEnv) { Set-ItResult -Skipped -Because 'the Unity Catalog fixture was not provisioned' }
        }

        It 'should create a new schema' {
            $inputJson = @{
                name         = $script:testSchemaName
                catalog_name = $script:ucEnv.CatalogName
                comment      = 'Created by DSC test'
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Schema --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:testSchemaName
            $result.afterState.full_name | Should -Be "$($script:ucEnv.CatalogName).$($script:testSchemaName)"
            $result.afterState.comment | Should -Be 'Created by DSC test'
            $result.changedProperties | Should -Contain 'name'
            $script:schemaCreated = $true
        }

        It 'should verify the created schema via get' {
            if (-not $script:schemaCreated) { Set-ItResult -Skipped -Because 'the schema fixture was not created' }
            $inputJson = @{ name = $script:testSchemaName; catalog_name = $script:ucEnv.CatalogName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Schema --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.owner | Should -Not -BeNullOrEmpty
            $result.actualState.schema_id | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Set Operation - Update' -Tag 'Set', 'Update' {
        BeforeEach {
            if (-not $script:schemaCreated) { Set-ItResult -Skipped -Because 'the schema fixture was not created' }
        }

        It 'should update the comment of the schema' {
            $inputJson = @{
                name         = $script:testSchemaName
                catalog_name = $script:ucEnv.CatalogName
                comment      = 'Updated by DSC test'
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Schema --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState.comment | Should -Be 'Updated by DSC test'
            $result.changedProperties | Should -Contain 'comment'
        }

        It 'should verify the update via get' {
            $inputJson = @{ name = $script:testSchemaName; catalog_name = $script:ucEnv.CatalogName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Schema --input $inputJson | ConvertFrom-Json
            $result.actualState.comment | Should -Be 'Updated by DSC test'
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        BeforeEach {
            if (-not $script:schemaCreated) { Set-ItResult -Skipped -Because 'the schema fixture was not created' }
        }

        It 'should report inDesiredState=true when state matches' {
            $inputJson = @{
                name         = $script:testSchemaName
                catalog_name = $script:ucEnv.CatalogName
                comment      = 'Updated by DSC test'
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Schema --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $true
        }

        It 'should report inDesiredState=false when comment differs' {
            $inputJson = @{
                name         = $script:testSchemaName
                catalog_name = $script:ucEnv.CatalogName
                comment      = 'Different comment'
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Schema --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $false
        }
    }

    Context 'WhatIf Operation' -Tag 'WhatIf' {
        BeforeEach {
            if (-not $script:ucEnv) { Set-ItResult -Skipped -Because 'the Unity Catalog fixture was not provisioned' }
        }

        It 'should predict schema creation without creating anything' {
            $script:whatIfSchemaName = New-TestSchemaName
            $result = Invoke-DscWhatIf -ResourceType 'LibreDsc.Databricks/Schema' -Properties @{
                name         = $script:whatIfSchemaName
                catalog_name = $script:ucEnv.CatalogName
                comment      = 'whatif prediction'
            }
            $LASTEXITCODE | Should -Be 0
            $result.metadata.'Microsoft.DSC'.executionType | Should -Be 'whatIf'
            $result.results[0].result.afterState._exist | Should -Be $true
            $result.results[0].result.afterState.full_name | Should -Be "$($script:ucEnv.CatalogName).$($script:whatIfSchemaName)"
            $result.results[0].result.afterState.comment | Should -Be 'whatif prediction'
        }

        It 'should not have created the schema' {
            $inputJson = @{ name = $script:whatIfSchemaName; catalog_name = $script:ucEnv.CatalogName } | ConvertTo-Json -Compress
            $get = dsc resource get -r LibreDsc.Databricks/Schema --input $inputJson | ConvertFrom-Json
            $get.actualState._exist | Should -Be $false
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        BeforeEach {
            if (-not $script:schemaCreated) { Set-ItResult -Skipped -Because 'the schema fixture was not created' }
        }

        It 'should export schemas including the test schema' {
            $result = dsc resource export -r LibreDsc.Databricks/Schema | ConvertFrom-Json
            $result.resources | Should -Not -BeNullOrEmpty

            $s = $result.resources | Where-Object { $_.properties.full_name -eq "$($script:ucEnv.CatalogName).$($script:testSchemaName)" }
            $s | Should -Not -BeNullOrEmpty
            $s.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        BeforeEach {
            if (-not $script:schemaCreated) { Set-ItResult -Skipped -Because 'the schema fixture was not created' }
        }

        It 'should delete the test schema' {
            $inputJson = @{ name = $script:testSchemaName; catalog_name = $script:ucEnv.CatalogName } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/Schema --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the schema is gone via get' {
            $inputJson = @{ name = $script:testSchemaName; catalog_name = $script:ucEnv.CatalogName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Schema --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            if (-not $script:ucEnv) { return }
            $script:idempotentSchemaName = New-TestSchemaName
            $inputJson = @{
                name         = $script:idempotentSchemaName
                catalog_name = $script:ucEnv.CatalogName
                comment      = 'Idempotency test schema'
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/Schema --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentSchemaName -and $script:ucEnv)
            {
                try
                {
                    $inputJson = @{ name = $script:idempotentSchemaName; catalog_name = $script:ucEnv.CatalogName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/Schema --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            if (-not $script:idempotentSchemaName) { Set-ItResult -Skipped -Because 'the idempotency schema fixture was not created' }
            $inputJson = @{
                name         = $script:idempotentSchemaName
                catalog_name = $script:ucEnv.CatalogName
                comment      = 'Idempotency test schema'
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Schema --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.changedProperties | Should -BeNullOrEmpty
        }
    }
}
