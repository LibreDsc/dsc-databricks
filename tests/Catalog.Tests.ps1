[CmdletBinding()]
param (
    [Parameter()]
    [System.String]
    $ExeName = 'dsc-databricks'
)

BeforeDiscovery {
    . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
    $script:databricksAvailable = Initialize-DatabricksTests -ExeName $ExeName
    $script:catalogStorageAvailable = $script:databricksAvailable -and $env:DATABRICKS_CATALOG_STORAGE_LOCATION
}

Describe 'Databricks Catalog Resource' -Tag 'Databricks', 'Catalog' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testCatalogName = New-TestCatalogName
        $script:storageLocation = $env:DATABRICKS_CATALOG_STORAGE_LOCATION
    }

    AfterAll {
        if ($script:databricksAvailable -and $script:testCatalogName)
        {
            try
            {
                $inputJson = @{ name = $script:testCatalogName } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/Catalog --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/Catalog | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/Catalog'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/Catalog | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/Catalog | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.name | Should -Not -BeNullOrEmpty
            $result.properties.comment | Should -Not -BeNullOrEmpty
            $result.properties.owner | Should -Not -BeNullOrEmpty
            $result.properties.isolation_mode | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/Catalog | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should require name' {
            $result = dsc resource schema -r LibreDsc.Databricks/Catalog | ConvertFrom-Json
            $result.required | Should -Contain 'name'
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent catalog' {
            $inputJson = @{ name = 'dsc-nonexistent-catalog-000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Catalog --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set' -Skip:(!$script:catalogStorageAvailable) {
        It 'should create a new catalog' {
            $inputJson = @{
                name         = $script:testCatalogName
                comment      = 'Created by DSC test'
                storage_root = $script:storageLocation
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Catalog --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:testCatalogName
            $result.afterState.comment | Should -Be 'Created by DSC test'
            $result.changedProperties | Should -Contain '_exist'
        }

        It 'should verify the created catalog via get' {
            $inputJson = @{ name = $script:testCatalogName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Catalog --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.name | Should -Be $script:testCatalogName
            $result.actualState.comment | Should -Be 'Created by DSC test'
            $result.actualState.owner | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Set Operation - Update' -Tag 'Set' -Skip:(!$script:catalogStorageAvailable) {
        It 'should update the comment of the catalog' {
            $inputJson = @{
                name    = $script:testCatalogName
                comment = 'Updated by DSC test'
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Catalog --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.comment | Should -Be 'Updated by DSC test'
            $result.changedProperties | Should -Contain 'comment'
        }

        It 'should verify the update via get' {
            $inputJson = @{ name = $script:testCatalogName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Catalog --input $inputJson | ConvertFrom-Json
            $result.actualState.comment | Should -Be 'Updated by DSC test'
        }
    }

    Context 'Test Operation' -Tag 'Test' -Skip:(!$script:catalogStorageAvailable) {
        It 'should report inDesiredState=true when state matches' {
            $inputJson = @{
                name    = $script:testCatalogName
                comment = 'Updated by DSC test'
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Catalog --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $true
            $result.differingProperties | Should -BeNullOrEmpty
        }

        It 'should report inDesiredState=false when comment differs' {
            $inputJson = @{
                name    = $script:testCatalogName
                comment = 'Different comment'
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Catalog --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $false
            $result.differingProperties | Should -Contain 'comment'
        }
    }

    Context 'Export Operation' -Tag 'Export' -Skip:(!$script:catalogStorageAvailable) {
        It 'should export catalogs including the test catalog' {
            $result = dsc resource export -r LibreDsc.Databricks/Catalog | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty

            $c = $result.resources | Where-Object { $_.properties.name -eq $script:testCatalogName }
            $c | Should -Not -BeNullOrEmpty
            $c.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' -Skip:(!$script:catalogStorageAvailable) {
        It 'should delete the test catalog' {
            $inputJson = @{ name = $script:testCatalogName } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/Catalog --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the catalog is gone via get' {
            $inputJson = @{ name = $script:testCatalogName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Catalog --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' -Skip:(!$script:catalogStorageAvailable) {
        BeforeAll {
            $script:idempotentCatalogName = New-TestCatalogName
            $inputJson = @{
                name         = $script:idempotentCatalogName
                comment      = 'Idempotency test catalog'
                storage_root = $script:storageLocation
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/Catalog --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentCatalogName)
            {
                try
                {
                    $inputJson = @{ name = $script:idempotentCatalogName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/Catalog --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            $inputJson = @{
                name    = $script:idempotentCatalogName
                comment = 'Idempotency test catalog'
            } | ConvertTo-Json -Compress
            # Note: storage_root is omitted here — the catalog already exists so Set triggers Update, not Create.

            $result = dsc resource set -r LibreDsc.Databricks/Catalog --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:idempotentCatalogName
            $result.changedProperties | Should -BeNullOrEmpty
        }
    }
}
