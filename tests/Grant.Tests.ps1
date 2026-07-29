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

Describe 'Databricks Grant Resource' -Tag 'Databricks', 'Grant', 'UnityCatalog' -Skip:(!$script:unityCatalogStorageAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:ucEnv = New-UnityCatalogTestEnvironment
        # The current user is a principal guaranteed to exist for both PAT
        # and AAD-token authentication.
        $script:principal = Get-DatabricksCurrentUserName
    }

    AfterAll {
        Remove-UnityCatalogTestEnvironment -Environment $script:ucEnv
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/Grant | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/Grant'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/Grant | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'test'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
            $result.capabilities | Should -Contain 'setWhatIf'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/Grant | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.securable_type | Should -Not -BeNullOrEmpty
            $result.properties.full_name | Should -Not -BeNullOrEmpty
            $result.properties.principal | Should -Not -BeNullOrEmpty
            $result.properties.privileges | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/Grant | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should require securable_type, full_name, and principal' {
            $result = dsc resource schema -r LibreDsc.Databricks/Grant | ConvertFrom-Json
            $result.required | Should -Contain 'securable_type'
            $result.required | Should -Contain 'full_name'
            $result.required | Should -Contain 'principal'
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        BeforeEach {
            if (-not $script:ucEnv) { Set-ItResult -Skipped -Because 'the Unity Catalog fixture was not provisioned' }
        }

        It 'should return _exist=false when the principal has no direct grants' {
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
            } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Grant --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set', 'Create' {
        BeforeEach {
            if (-not $script:ucEnv) { Set-ItResult -Skipped -Because 'the Unity Catalog fixture was not provisioned' }
        }

        It 'should grant USE_CATALOG on the fixture catalog' {
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
                privileges     = @('USE_CATALOG')
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Grant --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.privileges | Should -Be @('USE_CATALOG')
            $result.changedProperties | Should -Not -BeNullOrEmpty
            $script:grantCreated = $true
        }

        It 'should verify the grant via get' {
            if (-not $script:grantCreated) { Set-ItResult -Skipped -Because 'the grant fixture was not created' }
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
            } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Grant --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.privileges | Should -Contain 'USE_CATALOG'
        }
    }

    Context 'Set Operation - Update' -Tag 'Set', 'Update' {
        BeforeEach {
            if (-not $script:grantCreated) { Set-ItResult -Skipped -Because 'the grant fixture was not created' }
        }

        It 'should converge to an extended privilege set' {
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
                privileges     = @('USE_CATALOG', 'CREATE_SCHEMA')
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Grant --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            # Privileges come back in canonical sorted order.
            $result.afterState.privileges | Should -Be @('CREATE_SCHEMA', 'USE_CATALOG')
            $result.changedProperties | Should -Contain 'privileges'
        }

        It 'should remove privileges not in the desired set' {
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
                privileges     = @('USE_CATALOG')
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Grant --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState.privileges | Should -Be @('USE_CATALOG')
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        BeforeAll {
            if (-not $script:grantCreated) { return }
            # Converge to a two-privilege set for the order-insensitivity check.
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
                privileges     = @('USE_CATALOG', 'BROWSE')
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/Grant --input $inputJson | Out-Null
        }

        BeforeEach {
            if (-not $script:grantCreated) { Set-ItResult -Skipped -Because 'the grant fixture was not created' }
        }

        It 'should be order-insensitive for the privileges list' {
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
                privileges     = @('USE_CATALOG', 'BROWSE')
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Grant --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $true
            $result.differingProperties | Should -BeNullOrEmpty
        }

        It 'should report inDesiredState=false when privileges differ' {
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
                privileges     = @('USE_CATALOG', 'CREATE_VOLUME')
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Grant --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $false
            $result.differingProperties | Should -Contain 'privileges'
        }
    }

    Context 'WhatIf Operation' -Tag 'WhatIf' {
        BeforeEach {
            if (-not $script:grantCreated) { Set-ItResult -Skipped -Because 'the grant fixture was not created' }
        }

        It 'should predict the resulting privilege set without applying it' {
            $result = Invoke-DscWhatIf -ResourceType 'LibreDsc.Databricks/Grant' -Properties @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
                privileges     = @('USE_CATALOG', 'CREATE_FUNCTION')
            }
            $LASTEXITCODE | Should -Be 0
            $result.metadata.'Microsoft.DSC'.executionType | Should -Be 'whatIf'
            $result.results[0].result.afterState._exist | Should -Be $true
            $result.results[0].result.afterState.privileges | Should -Be @('CREATE_FUNCTION', 'USE_CATALOG')
        }

        It 'should not have changed the real grants' {
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
            } | ConvertTo-Json -Compress
            $get = dsc resource get -r LibreDsc.Databricks/Grant --input $inputJson | ConvertFrom-Json
            $get.actualState.privileges | Should -Not -Contain 'CREATE_FUNCTION'
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        BeforeEach {
            if (-not $script:grantCreated) { Set-ItResult -Skipped -Because 'the grant fixture was not created' }
        }

        It 'should export grants including the test grant' {
            $result = dsc resource export -r LibreDsc.Databricks/Grant | ConvertFrom-Json
            $result.resources | Should -Not -BeNullOrEmpty

            $g = $result.resources | Where-Object {
                $_.properties.securable_type -eq 'catalog' -and
                $_.properties.full_name -eq $script:ucEnv.CatalogName -and
                $_.properties.principal -eq $script:principal
            }
            $g | Should -Not -BeNullOrEmpty
            $g.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        BeforeEach {
            if (-not $script:grantCreated) { Set-ItResult -Skipped -Because 'the grant fixture was not created' }
        }

        It 'should revoke all privileges for the principal' {
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
            } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/Grant --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the grant is gone via get' {
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
            } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Grant --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            if (-not $script:ucEnv) { return }
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
                privileges     = @('BROWSE')
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/Grant --input $inputJson | Out-Null
            $script:idempotencyGrantCreated = $LASTEXITCODE -eq 0
        }

        AfterAll {
            if ($script:idempotencyGrantCreated)
            {
                try
                {
                    $inputJson = @{
                        securable_type = 'catalog'
                        full_name      = $script:ucEnv.CatalogName
                        principal      = $script:principal
                    } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/Grant --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            if (-not $script:idempotencyGrantCreated) { Set-ItResult -Skipped -Because 'the idempotency grant fixture was not created' }
            $inputJson = @{
                securable_type = 'catalog'
                full_name      = $script:ucEnv.CatalogName
                principal      = $script:principal
                privileges     = @('BROWSE')
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Grant --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.changedProperties | Should -BeNullOrEmpty
        }
    }
}
