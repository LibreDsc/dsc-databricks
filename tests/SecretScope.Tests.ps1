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

Describe 'Databricks SecretScope Resource' -Tag 'Databricks', 'SecretScope' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testScopeName = New-TestScopeName
    }

    AfterAll {
        # Cleanup: delete the test scope if it exists
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
            $result = dsc resource list LibreDsc.Databricks/SecretScope | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/SecretScope'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/SecretScope | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/SecretScope | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.scope | Should -Not -BeNullOrEmpty
            $result.properties.backend_type | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/SecretScope | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for non-existent scope' {
            $inputJson = @{ scope = 'nonexistent-scope-dsc-test-000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SecretScope --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create Scope' -Tag 'Set' {
        It 'should create a new secret scope' {
            $inputJson = @{
                scope = $script:testScopeName
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/SecretScope --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.scope | Should -Be $script:testScopeName
        }

        It 'should verify the created scope via get' {
            $inputJson = @{ scope = $script:testScopeName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SecretScope --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.scope | Should -Be $script:testScopeName
            $result.actualState.backend_type | Should -Be 'DATABRICKS'
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        It 'should report in desired state when matching' {
            $inputJson = @{
                scope        = $script:testScopeName
                backend_type = 'DATABRICKS'
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/SecretScope --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $true
            $result.differingProperties | Should -BeNullOrEmpty
        }

        It 'should report not in desired state when _exist differs' {
            $inputJson = @{
                scope  = 'nonexistent-scope-dsc-test-000'
                _exist = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/SecretScope --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $false
            $result.differingProperties | Should -Contain '_exist'
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        It 'should export scopes with resources' {
            $result = dsc resource export -r LibreDsc.Databricks/SecretScope | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty
            $result.resources.Count | Should -BeGreaterOrEqual 1
        }

        It 'should include the test scope in export results' {
            $result = dsc resource export -r LibreDsc.Databricks/SecretScope | ConvertFrom-Json
            $found = $result.resources | Where-Object { $_.properties.scope -eq $script:testScopeName }
            $found | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        It 'should delete the test scope' {
            $inputJson = @{ scope = $script:testScopeName } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/SecretScope --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0

            # Verify deletion
            $result = dsc resource get -r LibreDsc.Databricks/SecretScope --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $script:idempotentScope = New-TestScopeName
            $inputJson = @{ scope = $script:idempotentScope } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/SecretScope --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentScope)
            {
                try
                {
                    $inputJson = @{ scope = $script:idempotentScope } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/SecretScope --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when setting the same scope twice' {
            $inputJson = @{ scope = $script:idempotentScope } | ConvertTo-Json -Compress

            dsc resource set -r LibreDsc.Databricks/SecretScope --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0

            dsc resource set -r LibreDsc.Databricks/SecretScope --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0

            $result = dsc resource get -r LibreDsc.Databricks/SecretScope --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.scope | Should -Be $script:idempotentScope
        }
    }
}
