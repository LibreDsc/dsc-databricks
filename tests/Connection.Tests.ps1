[CmdletBinding()]
param (
    [Parameter()]
    [System.String]
    $ExeName = 'dsc-databricks'
)

BeforeDiscovery {
    . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
    $script:databricksAvailable = Initialize-DatabricksTests -ExeName $ExeName
    $script:unityCatalogAvailable = $script:databricksAvailable -and (Test-UnityCatalogAvailable)
}

Describe 'Databricks Connection Resource' -Tag 'Databricks', 'Connection', 'UnityCatalog' -Skip:(!$script:unityCatalogAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testConnectionName = New-TestConnectionName
        # HTTP connections are not connectivity-validated at create time. If a
        # region starts validating these options, switch to connection_type
        # DATABRICKS pointing at the test workspace itself (real host + token).
        $script:connectionOptions = @{
            host         = 'https://example.com'
            port         = '443'
            base_path    = '/api'
            bearer_token = 'dsc-test-dummy-token'
        }
    }

    AfterAll {
        if ($script:testConnectionName)
        {
            try
            {
                $inputJson = @{ name = $script:testConnectionName } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/Connection --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/Connection | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/Connection'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/Connection | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
            $result.capabilities | Should -Contain 'setWhatIf'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/Connection | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.name | Should -Not -BeNullOrEmpty
            $result.properties.connection_type | Should -Not -BeNullOrEmpty
            $result.properties.options | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/Connection | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should require only name' {
            $result = dsc resource schema -r LibreDsc.Databricks/Connection | ConvertFrom-Json
            $result.required | Should -Be @('name')
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent connection' {
            $inputJson = @{ name = 'dsc-nonexistent-connection-000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Connection --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set', 'Create' {
        It 'should create a new HTTP connection' {
            $inputJson = @{
                name            = $script:testConnectionName
                connection_type = 'HTTP'
                comment         = 'Created by DSC test'
                options         = $script:connectionOptions
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Connection --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:testConnectionName
            $result.afterState.connection_type | Should -Be 'HTTP'
            $result.changedProperties | Should -Contain 'name'
            $script:connectionCreated = $true
        }

        It 'should verify the created connection via get' {
            if (-not $script:connectionCreated) { Set-ItResult -Skipped -Because 'the connection fixture was not created' }
            $inputJson = @{ name = $script:testConnectionName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Connection --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.connection_id | Should -Not -BeNullOrEmpty
            $result.actualState.comment | Should -Be 'Created by DSC test'
            $result.actualState.options.host | Should -Be 'https://example.com'
        }
    }

    Context 'Set Operation - Update' -Tag 'Set', 'Update' {
        BeforeEach {
            if (-not $script:connectionCreated) { Set-ItResult -Skipped -Because 'the connection fixture was not created' }
        }

        It 'should update the options with a full map resend' {
            $updatedOptions = $script:connectionOptions.Clone()
            $updatedOptions.base_path = '/api/v2'
            $inputJson = @{
                name            = $script:testConnectionName
                connection_type = 'HTTP'
                options         = $updatedOptions
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Connection --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState.options.base_path | Should -Be '/api/v2'
        }

        It 'should verify the update via get' {
            $inputJson = @{ name = $script:testConnectionName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Connection --input $inputJson | ConvertFrom-Json
            $result.actualState.options.base_path | Should -Be '/api/v2'
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        BeforeEach {
            if (-not $script:connectionCreated) { Set-ItResult -Skipped -Because 'the connection fixture was not created' }
        }

        It 'should report options drift when secret option values are specified' {
            # Secret option values (bearer_token) come back redacted from the
            # API, so a desired state containing them reports permanent drift
            # on options — same contract as the Secret resource's write-only
            # fields.
            $updatedOptions = $script:connectionOptions.Clone()
            $updatedOptions.base_path = '/api/v2'
            $inputJson = @{
                name            = $script:testConnectionName
                connection_type = 'HTTP'
                options         = $updatedOptions
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Connection --input $inputJson | ConvertFrom-Json
            $result.differingProperties | Should -Contain 'options'
        }
    }

    Context 'WhatIf Operation' -Tag 'WhatIf' {
        It 'should predict connection creation without creating anything' {
            $script:whatIfConnectionName = New-TestConnectionName
            $result = Invoke-DscWhatIf -ResourceType 'LibreDsc.Databricks/Connection' -Properties @{
                name            = $script:whatIfConnectionName
                connection_type = 'HTTP'
                comment         = 'whatif prediction'
                options         = $script:connectionOptions
            }
            $LASTEXITCODE | Should -Be 0
            $result.metadata.'Microsoft.DSC'.executionType | Should -Be 'whatIf'
            $result.results[0].result.afterState._exist | Should -Be $true
            $result.results[0].result.afterState.connection_type | Should -Be 'HTTP'
        }

        It 'should not have created the connection' {
            $inputJson = @{ name = $script:whatIfConnectionName } | ConvertTo-Json -Compress
            $get = dsc resource get -r LibreDsc.Databricks/Connection --input $inputJson | ConvertFrom-Json
            $get.actualState._exist | Should -Be $false
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        BeforeEach {
            if (-not $script:connectionCreated) { Set-ItResult -Skipped -Because 'the connection fixture was not created' }
        }

        It 'should export connections including the test connection' {
            $result = dsc resource export -r LibreDsc.Databricks/Connection | ConvertFrom-Json
            $result.resources | Should -Not -BeNullOrEmpty

            $c = $result.resources | Where-Object { $_.properties.name -eq $script:testConnectionName }
            $c | Should -Not -BeNullOrEmpty
            $c.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        BeforeEach {
            if (-not $script:connectionCreated) { Set-ItResult -Skipped -Because 'the connection fixture was not created' }
        }

        It 'should delete the test connection' {
            $inputJson = @{ name = $script:testConnectionName } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/Connection --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the connection is gone via get' {
            $inputJson = @{ name = $script:testConnectionName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Connection --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $script:idempotentConnectionName = New-TestConnectionName
            $inputJson = @{
                name            = $script:idempotentConnectionName
                connection_type = 'HTTP'
                options         = $script:connectionOptions
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/Connection --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentConnectionName)
            {
                try
                {
                    $inputJson = @{ name = $script:idempotentConnectionName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/Connection --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            $inputJson = @{
                name            = $script:idempotentConnectionName
                connection_type = 'HTTP'
                options         = $script:connectionOptions
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Connection --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:idempotentConnectionName
        }
    }
}
