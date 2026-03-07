[CmdletBinding()]
param (
    [Parameter()]
    [System.String]
    $ExeName = 'dsc-databricks'
)

BeforeDiscovery {
    . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
    $script:databricksAvailable = Initialize-DatabricksTests -ExeName $ExeName
}

Describe 'Databricks WorkspaceConf Resource' -Tag 'Databricks', 'WorkspaceConf' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/WorkspaceConf | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/WorkspaceConf'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/WorkspaceConf | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/WorkspaceConf | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.key | Should -Not -BeNullOrEmpty
            $result.properties.value | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/WorkspaceConf | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should require key' {
            $result = dsc resource schema -r LibreDsc.Databricks/WorkspaceConf | ConvertFrom-Json
            $result.required | Should -Contain 'key'
        }
    }

    Context 'Get Operation - Boolean Key' -Tag 'Get' {
        It 'should get the enableTokensConfig key' {
            $inputJson = @{ key = 'enableTokensConfig' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.actualState._exist | Should -Be $true
            $result.actualState.key | Should -Be 'enableTokensConfig'
            $result.actualState.value | Should -BeIn @('true', 'false')
        }

        It 'should get the enableDbfsFileBrowser key' {
            $inputJson = @{ key = 'enableDbfsFileBrowser' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.actualState._exist | Should -Be $true
            $result.actualState.key | Should -Be 'enableDbfsFileBrowser'
            # Value may be null if key has not been explicitly configured yet.
        }
    }

    Context 'Get Operation - Integer Key' -Tag 'Get' {
        It 'should get the maxTokenLifetimeDays key' {
            $inputJson = @{ key = 'maxTokenLifetimeDays' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.actualState._exist | Should -Be $true
            $result.actualState.key | Should -Be 'maxTokenLifetimeDays'
            $result.actualState | Should -Not -Be $null
        }
    }

    Context 'Set Operation - Boolean Key' -Tag 'Set' {
        BeforeAll {
            # Save the current enableTokensConfig value so we can restore it.
            $inputJson = @{ key = 'enableTokensConfig' } | ConvertTo-Json -Compress
            $current = dsc resource get -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $script:originalEnableTokensConfig = $current.actualState.value
        }

        AfterAll {
            # Restore the original value.
            if ($null -ne $script:originalEnableTokensConfig) {
                try {
                    $inputJson = @{
                        key   = 'enableTokensConfig'
                        value = $script:originalEnableTokensConfig
                    } | ConvertTo-Json -Compress
                    dsc resource set -r LibreDsc.Databricks/WorkspaceConf --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should set enableTokensConfig to true' {
            $inputJson = @{
                key   = 'enableTokensConfig'
                value = 'true'
            } | ConvertTo-Json -Compress
            $result = dsc resource set -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.value | Should -Be 'true'
        }

        It 'should verify the updated value via get' {
            $inputJson = @{ key = 'enableTokensConfig' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $result.actualState.value | Should -Be 'true'
        }
    }

    Context 'Set Operation - Second Boolean Key' -Tag 'Set' {
        BeforeAll {
            # Save the current enableDbfsFileBrowser value so we can restore it.
            $inputJson = @{ key = 'enableDbfsFileBrowser' } | ConvertTo-Json -Compress
            $current = dsc resource get -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $script:originalEnableDbfsFileBrowser = $current.actualState.value
        }

        AfterAll {
            # Restore the original value.
            if ($null -ne $script:originalEnableDbfsFileBrowser) {
                try {
                    $inputJson = @{
                        key   = 'enableDbfsFileBrowser'
                        value = $script:originalEnableDbfsFileBrowser
                    } | ConvertTo-Json -Compress
                    dsc resource set -r LibreDsc.Databricks/WorkspaceConf --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should set enableDbfsFileBrowser to true' {
            $inputJson = @{
                key   = 'enableDbfsFileBrowser'
                value = 'true'
            } | ConvertTo-Json -Compress
            $result = dsc resource set -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.value | Should -Be 'true'
        }

        It 'should verify the updated enableDbfsFileBrowser via get' {
            $inputJson = @{ key = 'enableDbfsFileBrowser' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $result.actualState.value | Should -Be 'true'
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        It 'should report inDesiredState when key matches' {
            # Read current value, then test against it.
            $inputJson = @{ key = 'enableTokensConfig' } | ConvertTo-Json -Compress
            $current = dsc resource get -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $currentValue = $current.actualState.value

            $inputJson = @{
                key   = 'enableTokensConfig'
                value = $currentValue
            } | ConvertTo-Json -Compress
            $result = dsc resource test -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.inDesiredState | Should -Be $true
        }

        It 'should report NOT inDesiredState when value differs' {
            $inputJson = @{ key = 'enableTokensConfig' } | ConvertTo-Json -Compress
            $current = dsc resource get -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $currentValue = $current.actualState.value

            # Use the opposite value.
            $oppositeValue = if ($currentValue -eq 'true') { 'false' } else { 'true' }

            $inputJson = @{
                key   = 'enableTokensConfig'
                value = $oppositeValue
            } | ConvertTo-Json -Compress
            $result = dsc resource test -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.inDesiredState | Should -Be $false
            $result.differingProperties | Should -Contain 'value'
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        It 'should export workspace configuration keys' {
            $result = dsc resource export -r LibreDsc.Databricks/WorkspaceConf | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty
        }

        It 'should include known keys in export' {
            $result = dsc resource export -r LibreDsc.Databricks/WorkspaceConf | ConvertFrom-Json
            $keys = $result.resources | ForEach-Object { $_.properties.key }
            $keys | Should -Not -BeNullOrEmpty
            # At least enableTokensConfig should be present.
            $keys | Should -Contain 'enableTokensConfig'
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        It 'should be idempotent when key is already at desired value' {
            # Read current value.
            $inputJson = @{ key = 'enableTokensConfig' } | ConvertTo-Json -Compress
            $current = dsc resource get -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $currentValue = $current.actualState.value

            # Set to the same value twice.
            $inputJson = @{
                key   = 'enableTokensConfig'
                value = $currentValue
            } | ConvertTo-Json -Compress

            $result1 = dsc resource set -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result1.afterState.value | Should -Be $currentValue

            $result2 = dsc resource set -r LibreDsc.Databricks/WorkspaceConf --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result2.afterState.value | Should -Be $currentValue
        }
    }
}
