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

Describe 'Databricks Volume Resource' -Tag 'Databricks', 'Volume', 'UnityCatalog' -Skip:(!$script:unityCatalogStorageAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:ucEnv = New-UnityCatalogTestEnvironment
        $script:testVolumeName = New-TestVolumeName
    }

    AfterAll {
        # Force-deleting the fixture catalog cascades over the volumes.
        Remove-UnityCatalogTestEnvironment -Environment $script:ucEnv
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/Volume | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/Volume'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/Volume | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
            $result.capabilities | Should -Contain 'setWhatIf'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/Volume | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.name | Should -Not -BeNullOrEmpty
            $result.properties.catalog_name | Should -Not -BeNullOrEmpty
            $result.properties.schema_name | Should -Not -BeNullOrEmpty
            $result.properties.volume_type | Should -Not -BeNullOrEmpty
            $result.properties.storage_location | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/Volume | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should require name, catalog_name, and schema_name' {
            $result = dsc resource schema -r LibreDsc.Databricks/Volume | ConvertFrom-Json
            $result.required | Should -Contain 'name'
            $result.required | Should -Contain 'catalog_name'
            $result.required | Should -Contain 'schema_name'
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent volume' {
            if (-not $script:ucEnv) { Set-ItResult -Skipped -Because 'the Unity Catalog fixture was not provisioned' }
            $inputJson = @{
                name         = 'dsc_nonexistent_volume_000'
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
            } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Volume --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set', 'Create' {
        BeforeEach {
            if (-not $script:ucEnv) { Set-ItResult -Skipped -Because 'the Unity Catalog fixture was not provisioned' }
        }

        It 'should create a new managed volume' {
            $inputJson = @{
                name         = $script:testVolumeName
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
                volume_type  = 'MANAGED'
                comment      = 'Created by DSC test'
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Volume --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:testVolumeName
            $result.afterState.volume_type | Should -Be 'MANAGED'
            $result.afterState.full_name | Should -Be "$($script:ucEnv.CatalogName).$($script:ucEnv.SchemaName).$($script:testVolumeName)"
            $result.changedProperties | Should -Contain 'name'
            $script:volumeCreated = $true
        }

        It 'should verify the created volume via get' {
            if (-not $script:volumeCreated) { Set-ItResult -Skipped -Because 'the volume fixture was not created' }
            $inputJson = @{
                name         = $script:testVolumeName
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
            } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Volume --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.comment | Should -Be 'Created by DSC test'
            $result.actualState.volume_id | Should -Not -BeNullOrEmpty
            # Managed volumes get a server-assigned storage location.
            $result.actualState.storage_location | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Set Operation - Update' -Tag 'Set', 'Update' {
        BeforeEach {
            if (-not $script:volumeCreated) { Set-ItResult -Skipped -Because 'the volume fixture was not created' }
        }

        It 'should update the comment of the volume' {
            $inputJson = @{
                name         = $script:testVolumeName
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
                volume_type  = 'MANAGED'
                comment      = 'Updated by DSC test'
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Volume --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState.comment | Should -Be 'Updated by DSC test'
            $result.changedProperties | Should -Contain 'comment'
        }

        It 'should verify the update via get' {
            $inputJson = @{
                name         = $script:testVolumeName
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
            } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Volume --input $inputJson | ConvertFrom-Json
            $result.actualState.comment | Should -Be 'Updated by DSC test'
        }
    }

    Context 'WhatIf Operation' -Tag 'WhatIf' {
        BeforeEach {
            if (-not $script:ucEnv) { Set-ItResult -Skipped -Because 'the Unity Catalog fixture was not provisioned' }
        }

        It 'should predict volume creation without creating anything' {
            $script:whatIfVolumeName = New-TestVolumeName
            $result = Invoke-DscWhatIf -ResourceType 'LibreDsc.Databricks/Volume' -Properties @{
                name         = $script:whatIfVolumeName
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
                volume_type  = 'MANAGED'
                comment      = 'whatif prediction'
            }
            $LASTEXITCODE | Should -Be 0
            $result.metadata.'Microsoft.DSC'.executionType | Should -Be 'whatIf'
            $result.results[0].result.afterState._exist | Should -Be $true
            $result.results[0].result.afterState.volume_type | Should -Be 'MANAGED'
            $result.results[0].result.afterState.comment | Should -Be 'whatif prediction'
        }

        It 'should not have created the volume' {
            $inputJson = @{
                name         = $script:whatIfVolumeName
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
            } | ConvertTo-Json -Compress
            $get = dsc resource get -r LibreDsc.Databricks/Volume --input $inputJson | ConvertFrom-Json
            $get.actualState._exist | Should -Be $false
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        BeforeEach {
            if (-not $script:volumeCreated) { Set-ItResult -Skipped -Because 'the volume fixture was not created' }
        }

        It 'should export volumes including the test volume' {
            $result = dsc resource export -r LibreDsc.Databricks/Volume | ConvertFrom-Json
            $result.resources | Should -Not -BeNullOrEmpty

            $v = $result.resources | Where-Object {
                $_.properties.full_name -eq "$($script:ucEnv.CatalogName).$($script:ucEnv.SchemaName).$($script:testVolumeName)"
            }
            $v | Should -Not -BeNullOrEmpty
            $v.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        BeforeEach {
            if (-not $script:volumeCreated) { Set-ItResult -Skipped -Because 'the volume fixture was not created' }
        }

        It 'should delete the test volume' {
            $inputJson = @{
                name         = $script:testVolumeName
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
            } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/Volume --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the volume is gone via get' {
            $inputJson = @{
                name         = $script:testVolumeName
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
            } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Volume --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            if (-not $script:ucEnv) { return }
            $script:idempotentVolumeName = New-TestVolumeName
            $inputJson = @{
                name         = $script:idempotentVolumeName
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
                volume_type  = 'MANAGED'
                comment      = 'Idempotency test volume'
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/Volume --input $inputJson | Out-Null
        }

        It 'should be idempotent when set is called again with the same desired state' {
            if (-not $script:idempotentVolumeName) { Set-ItResult -Skipped -Because 'the idempotency volume fixture was not created' }
            $inputJson = @{
                name         = $script:idempotentVolumeName
                catalog_name = $script:ucEnv.CatalogName
                schema_name  = $script:ucEnv.SchemaName
                volume_type  = 'MANAGED'
                comment      = 'Idempotency test volume'
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Volume --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.changedProperties | Should -BeNullOrEmpty
        }
    }
}
