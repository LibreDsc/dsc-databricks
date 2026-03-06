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

Describe 'Databricks ClusterPolicy Resource' -Tag 'Databricks', 'ClusterPolicy' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testPolicyName = New-TestClusterPolicyName

        $testdataDir = Join-Path $PSScriptRoot 'testdata'
        $script:definition        = (Get-Content (Join-Path $testdataDir 'cluster-policy-definition.json') -Raw | ConvertFrom-Json | ConvertTo-Json -Compress)
        $script:definitionUpdated = (Get-Content (Join-Path $testdataDir 'cluster-policy-definition-updated.json') -Raw | ConvertFrom-Json | ConvertTo-Json -Compress)
    }

    AfterAll {
        if ($script:databricksAvailable -and $script:testPolicyName)
        {
            try
            {
                $inputJson = @{ name = $script:testPolicyName } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/ClusterPolicy --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/ClusterPolicy | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/ClusterPolicy'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/ClusterPolicy | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/ClusterPolicy | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.name | Should -Not -BeNullOrEmpty
            $result.properties.policy_id | Should -Not -BeNullOrEmpty
            $result.properties.definition | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/ClusterPolicy | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent policy' {
            $inputJson = @{ name = 'dsc-nonexistent-policy-000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ClusterPolicy --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set' {
        It 'should create a new cluster policy' {
            $inputJson = @{
                name       = $script:testPolicyName
                definition = $script:definition
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ClusterPolicy --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:testPolicyName
            $result.afterState.policy_id | Should -Not -BeNullOrEmpty
            $result.changedProperties | Should -Contain '_exist'
            $script:testPolicyId = $result.afterState.policy_id
        }

        It 'should verify the created policy via get' {
            $inputJson = @{ policy_id = $script:testPolicyId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ClusterPolicy --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.name | Should -Be $script:testPolicyName
        }
    }

    Context 'Set Operation - Update' -Tag 'Set' {
        It 'should update the description of the policy' {
            $inputJson = @{
                policy_id   = $script:testPolicyId
                name        = $script:testPolicyName
                definition  = $script:definitionUpdated
                description = 'Updated by DSC test'
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ClusterPolicy --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.description | Should -Be 'Updated by DSC test'
            $result.changedProperties | Should -Contain 'description'
        }

        It 'should verify the update via get' {
            $inputJson = @{ policy_id = $script:testPolicyId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ClusterPolicy --input $inputJson | ConvertFrom-Json
            $result.actualState.description | Should -Be 'Updated by DSC test'
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        It 'should export cluster policies and include the test policy' {
            $result = dsc resource export -r LibreDsc.Databricks/ClusterPolicy | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty

            $p = $result.resources | Where-Object { $_.properties.name -eq $script:testPolicyName }
            $p | Should -Not -BeNullOrEmpty
            $p.properties._exist | Should -Be $true
        }

        It 'should not export default built-in policies' {
            $result = dsc resource export -r LibreDsc.Databricks/ClusterPolicy | ConvertFrom-Json
            $defaultPolicies = $result.resources | Where-Object { $_.properties.name -eq 'Personal Compute' -or $_.properties.name -eq 'Shared Compute' }
            $defaultPolicies | Should -BeNullOrEmpty
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        It 'should delete the test policy by policy_id' {
            $inputJson = @{ policy_id = $script:testPolicyId } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/ClusterPolicy --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the policy is gone via get' {
            $inputJson = @{ policy_id = $script:testPolicyId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ClusterPolicy --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $script:idempotentPolicyName = New-TestClusterPolicyName
            $inputJson = @{
                name       = $script:idempotentPolicyName
                definition = $script:definition
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/ClusterPolicy --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentPolicyName)
            {
                try
                {
                    $inputJson = @{ name = $script:idempotentPolicyName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/ClusterPolicy --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            $inputJson = @{
                name       = $script:idempotentPolicyName
                definition = $script:definition
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ClusterPolicy --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:idempotentPolicyName
            $result.changedProperties | Should -BeNullOrEmpty
        }
    }
}
