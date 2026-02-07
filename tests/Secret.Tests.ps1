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

Describe 'Databricks Secret Resource' -Tag 'Databricks', 'Secret' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        # Create a dedicated scope for secret tests
        $script:testScopeName = New-TestScopeName
        $inputJson = @{ scope = $script:testScopeName } | ConvertTo-Json -Compress
        dsc resource set -r LibreDsc.Databricks/SecretScope --input $inputJson | Out-Null

        $script:testSecretKey = "dsc-test-secret-$([guid]::NewGuid().ToString('N').Substring(0,8))"
        $script:testSecretValue = 'dsc-test-secret-value'
        $script:testSecretValueUpdated = 'dsc-test-secret-value-updated'
    }

    AfterAll {
        # Cleanup: delete the entire test scope (which also deletes secrets within it)
        if ($script:databricksAvailable -and $script:testScopeName)
        {
            try
            {
                $inputJson = @{ scope = $script:testScopeName } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/SecretScope --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/Secret | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/Secret'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/Secret | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/Secret | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.scope | Should -Not -BeNullOrEmpty
            $result.properties.key | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/Secret | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should include string_value and bytes_value properties' {
            $result = dsc resource schema -r LibreDsc.Databricks/Secret | ConvertFrom-Json
            $result.properties.string_value | Should -Not -BeNullOrEmpty
            $result.properties.bytes_value | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for non-existent secret' {
            $inputJson = @{
                scope = $script:testScopeName
                key   = 'nonexistent-key-dsc-test-000'
            } | ConvertTo-Json -Compress

            $result = dsc resource get -r LibreDsc.Databricks/Secret --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create Secret' -Tag 'Set' {
        It 'should create a new secret' {
            $inputJson = @{
                scope        = $script:testScopeName
                key          = $script:testSecretKey
                string_value = $script:testSecretValue
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Secret --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.scope | Should -Be $script:testScopeName
            $result.afterState.key | Should -Be $script:testSecretKey
        }

        It 'should verify the created secret via get' {
            $inputJson = @{
                scope = $script:testScopeName
                key   = $script:testSecretKey
            } | ConvertTo-Json -Compress

            $result = dsc resource get -r LibreDsc.Databricks/Secret --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.scope | Should -Be $script:testScopeName
            $result.actualState.key | Should -Be $script:testSecretKey
        }
    }

    Context 'Set Operation - Update Secret' -Tag 'Set' {
        It 'should update the secret value' {
            $inputJson = @{
                scope        = $script:testScopeName
                key          = $script:testSecretKey
                string_value = $script:testSecretValueUpdated
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Secret --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        It 'should report in desired state when secret exists' {
            $inputJson = @{
                scope = $script:testScopeName
                key   = $script:testSecretKey
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Secret --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $true
        }

        It 'should report not in desired state when secret does not exist' {
            $inputJson = @{
                scope = $script:testScopeName
                key   = 'nonexistent-key-dsc-test-000'
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Secret --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $false
            $result.differingProperties | Should -Contain '_exist'
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        It 'should return secrets including the test secret' {
            $result = dsc resource export -r LibreDsc.Databricks/Secret | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $found = $result.resources | Where-Object { $_.properties.scope -eq $script:testScopeName -and $_.properties.key -eq $script:testSecretKey }
            $found | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        It 'should delete the test secret' {
            $inputJson = @{
                scope = $script:testScopeName
                key   = $script:testSecretKey
            } | ConvertTo-Json -Compress

            dsc resource delete -r LibreDsc.Databricks/Secret --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0

            # Verify deletion
            $result = dsc resource get -r LibreDsc.Databricks/Secret --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $script:idempotentKey = "dsc-idempotent-$([guid]::NewGuid().ToString('N').Substring(0,8))"
            $inputJson = @{
                scope        = $script:testScopeName
                key          = $script:idempotentKey
                string_value = 'idempotent-value'
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/Secret --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentKey)
            {
                try
                {
                    $inputJson = @{
                        scope = $script:testScopeName
                        key   = $script:idempotentKey
                    } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/Secret --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when setting the same secret twice' {
            $inputJson = @{
                scope        = $script:testScopeName
                key          = $script:idempotentKey
                string_value = 'idempotent-value'
            } | ConvertTo-Json -Compress

            dsc resource set -r LibreDsc.Databricks/Secret --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0

            dsc resource set -r LibreDsc.Databricks/Secret --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0

            $verifyJson = @{
                scope = $script:testScopeName
                key   = $script:idempotentKey
            } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Secret --input $verifyJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
        }
    }
}
