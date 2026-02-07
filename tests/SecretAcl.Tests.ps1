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

Describe 'Databricks SecretAcl Resource' -Tag 'Databricks', 'SecretAcl' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        # Create a dedicated scope for ACL tests
        $script:testScopeName = New-TestScopeName
        $inputJson = @{ scope = $script:testScopeName } | ConvertTo-Json -Compress
        dsc resource set -r LibreDsc.Databricks/SecretScope --input $inputJson | Out-Null

        $script:testPrincipal = 'users'
        $script:testPermission = 'READ'
        $script:testPermissionUpdated = 'MANAGE'
    }

    AfterAll {
        # Cleanup: delete the entire test scope (cascades ACLs)
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
            $result = dsc resource list LibreDsc.Databricks/SecretAcl | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/SecretAcl'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/SecretAcl | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/SecretAcl | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.scope | Should -Not -BeNullOrEmpty
            $result.properties.principal | Should -Not -BeNullOrEmpty
            $result.properties.permission | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/SecretAcl | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for non-existent ACL' {
            $inputJson = @{
                scope     = $script:testScopeName
                principal = 'nonexistent-principal-dsc-test'
            } | ConvertTo-Json -Compress

            $result = dsc resource get -r LibreDsc.Databricks/SecretAcl --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create ACL' -Tag 'Set' {
        It 'should create a new ACL entry' {
            $inputJson = @{
                scope      = $script:testScopeName
                principal  = $script:testPrincipal
                permission = $script:testPermission
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/SecretAcl --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.scope | Should -Be $script:testScopeName
            $result.afterState.principal | Should -Be $script:testPrincipal
            $result.afterState.permission | Should -Be $script:testPermission
        }

        It 'should verify the created ACL via get' {
            $inputJson = @{
                scope     = $script:testScopeName
                principal = $script:testPrincipal
            } | ConvertTo-Json -Compress

            $result = dsc resource get -r LibreDsc.Databricks/SecretAcl --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.scope | Should -Be $script:testScopeName
            $result.actualState.principal | Should -Be $script:testPrincipal
            $result.actualState.permission | Should -Be $script:testPermission
        }
    }

    Context 'Set Operation - Update ACL' -Tag 'Set' {
        It 'should update the ACL permission' {
            $inputJson = @{
                scope      = $script:testScopeName
                principal  = $script:testPrincipal
                permission = $script:testPermissionUpdated
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/SecretAcl --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0

            # Verify the update
            $verifyJson = @{
                scope     = $script:testScopeName
                principal = $script:testPrincipal
            } | ConvertTo-Json -Compress
            $verify = dsc resource get -r LibreDsc.Databricks/SecretAcl --input $verifyJson | ConvertFrom-Json
            $verify.actualState.permission | Should -Be $script:testPermissionUpdated
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        It 'should report in desired state when matching' {
            $inputJson = @{
                scope      = $script:testScopeName
                principal  = $script:testPrincipal
                permission = $script:testPermissionUpdated
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/SecretAcl --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $true
            $result.differingProperties | Should -BeNullOrEmpty
        }

        It 'should report not in desired state when permission differs' {
            $inputJson = @{
                scope      = $script:testScopeName
                principal  = $script:testPrincipal
                permission = 'WRITE'
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/SecretAcl --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $false
            $result.differingProperties | Should -Contain 'permission'
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        It 'should return ACL entries including the test ACL' {
            $result = dsc resource export -r LibreDsc.Databricks/SecretAcl | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $found = $result.resources | Where-Object {
                $_.properties.scope -eq $script:testScopeName -and $_.properties.principal -eq $script:testPrincipal
            }
            $found | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        It 'should delete the test ACL' {
            $inputJson = @{
                scope     = $script:testScopeName
                principal = $script:testPrincipal
            } | ConvertTo-Json -Compress

            dsc resource delete -r LibreDsc.Databricks/SecretAcl --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0

            # Verify deletion
            $result = dsc resource get -r LibreDsc.Databricks/SecretAcl --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $inputJson = @{
                scope      = $script:testScopeName
                principal  = $script:testPrincipal
                permission = 'READ'
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/SecretAcl --input $inputJson | Out-Null
        }

        AfterAll {
            try
            {
                $inputJson = @{
                    scope     = $script:testScopeName
                    principal = $script:testPrincipal
                } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/SecretAcl --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }

        It 'should be idempotent when setting the same ACL twice' {
            $inputJson = @{
                scope      = $script:testScopeName
                principal  = $script:testPrincipal
                permission = 'READ'
            } | ConvertTo-Json -Compress

            dsc resource set -r LibreDsc.Databricks/SecretAcl --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0

            dsc resource set -r LibreDsc.Databricks/SecretAcl --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0

            $verifyJson = @{
                scope     = $script:testScopeName
                principal = $script:testPrincipal
            } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SecretAcl --input $verifyJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.permission | Should -Be 'READ'
        }
    }
}
