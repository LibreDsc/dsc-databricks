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

Describe 'Databricks WorkspaceSetting Resource' -Tag 'Databricks', 'WorkspaceSetting' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/WorkspaceSetting | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/WorkspaceSetting'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/WorkspaceSetting | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/WorkspaceSetting | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.setting_name | Should -Not -BeNullOrEmpty
            $result.properties.value | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/WorkspaceSetting | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should require setting_name' {
            $result = dsc resource schema -r LibreDsc.Databricks/WorkspaceSetting | ConvertFrom-Json
            $result.required | Should -Contain 'setting_name'
        }
    }

    Context 'Get Operation - Boolean Setting' -Tag 'Get' {
        It 'should get the sql_results_download setting' {
            $inputJson = @{ setting_name = 'sql_results_download' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.actualState._exist | Should -Be $true
            $result.actualState.setting_name | Should -Be 'sql_results_download'
            $result.actualState.value | Should -BeIn @('true', 'false')
        }

        It 'should get the dashboard_email_subscriptions setting' {
            $inputJson = @{ setting_name = 'dashboard_email_subscriptions' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.actualState._exist | Should -Be $true
            $result.actualState.setting_name | Should -Be 'dashboard_email_subscriptions'
            $result.actualState.value | Should -BeIn @('true', 'false')
        }
    }

    Context 'Get Operation - Enum Setting' -Tag 'Get' {
        It 'should get the restrict_workspace_admins setting' {
            $inputJson = @{ setting_name = 'restrict_workspace_admins' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.actualState._exist | Should -Be $true
            $result.actualState.setting_name | Should -Be 'restrict_workspace_admins'
            $result.actualState.value | Should -BeIn @('ALLOW_ALL', 'RESTRICT_TOKENS_AND_JOB_RUN_AS')
        }
    }

    Context 'Get Operation - String Setting' -Tag 'Get' {
        It 'should get the default_namespace setting' {
            $inputJson = @{ setting_name = 'default_namespace' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.actualState._exist | Should -Be $true
            $result.actualState.setting_name | Should -Be 'default_namespace'
            # value may be empty string if not configured
            $result.actualState.value | Should -Not -Be $null
        }
    }

    Context 'Set Operation - Boolean Setting' -Tag 'Set' {
        BeforeAll {
            # Save the current sql_results_download setting so we can restore it.
            $inputJson = @{ setting_name = 'sql_results_download' } | ConvertTo-Json -Compress
            $current = dsc resource get -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $script:originalSqlResultsDownload = $current.actualState.value
        }

        AfterAll {
            # Restore the original value.
            if ($script:originalSqlResultsDownload)
            {
                try
                {
                    $inputJson = @{
                        setting_name = 'sql_results_download'
                        value        = $script:originalSqlResultsDownload
                    } | ConvertTo-Json -Compress
                    dsc resource set -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should update sql_results_download to true' {
            $inputJson = @{
                setting_name = 'sql_results_download'
                value        = 'true'
            } | ConvertTo-Json -Compress
            $result = dsc resource set -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.value | Should -Be 'true'
        }

        It 'should verify the updated value via get' {
            $inputJson = @{ setting_name = 'sql_results_download' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $result.actualState.value | Should -Be 'true'
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        It 'should report inDesiredState when setting matches' {
            # First read the current value, then test against it.
            $inputJson = @{ setting_name = 'sql_results_download' } | ConvertTo-Json -Compress
            $current = dsc resource get -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $currentValue = $current.actualState.value

            $inputJson = @{
                setting_name = 'sql_results_download'
                value        = $currentValue
            } | ConvertTo-Json -Compress
            $result = dsc resource test -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.inDesiredState | Should -Be $true
        }

        It 'should report NOT inDesiredState when value differs' {
            $inputJson = @{ setting_name = 'sql_results_download' } | ConvertTo-Json -Compress
            $current = dsc resource get -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $currentValue = $current.actualState.value

            # Use the opposite value.
            $oppositeValue = if ($currentValue -eq 'true') { 'false' } else { 'true' }

            $inputJson = @{
                setting_name = 'sql_results_download'
                value        = $oppositeValue
            } | ConvertTo-Json -Compress
            $result = dsc resource test -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.inDesiredState | Should -Be $false
            $result.differingProperties | Should -Contain 'value'
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        It 'should export workspace settings' {
            $result = dsc resource export -r LibreDsc.Databricks/WorkspaceSetting | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty
        }

        It 'should include known settings in export' {
            $result = dsc resource export -r LibreDsc.Databricks/WorkspaceSetting | ConvertFrom-Json
            $settingNames = $result.resources | ForEach-Object { $_.properties.setting_name }
            # At least some settings should be present.
            $settingNames | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        It 'should be idempotent when setting is already at desired value' {
            # Read current value.
            $inputJson = @{ setting_name = 'sql_results_download' } | ConvertTo-Json -Compress
            $current = dsc resource get -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $currentValue = $current.actualState.value

            # Set to the same value twice.
            $inputJson = @{
                setting_name = 'sql_results_download'
                value        = $currentValue
            } | ConvertTo-Json -Compress

            $result1 = dsc resource set -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result1.afterState.value | Should -Be $currentValue

            $result2 = dsc resource set -r LibreDsc.Databricks/WorkspaceSetting --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result2.afterState.value | Should -Be $currentValue
        }
    }
}
