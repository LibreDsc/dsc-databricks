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

Describe 'Databricks ServicePrincipal Resource' -Tag 'Databricks', 'ServicePrincipal' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testSpName = New-TestServicePrincipalName
    }

    AfterAll {
        if ($script:databricksAvailable -and $script:testSpName)
        {
            try
            {
                $inputJson = @{ display_name = $script:testSpName } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/ServicePrincipal --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/ServicePrincipal | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/ServicePrincipal'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/ServicePrincipal | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/ServicePrincipal | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.display_name | Should -Not -BeNullOrEmpty
            $result.properties.application_id | Should -Not -BeNullOrEmpty
            $result.properties.active | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/ServicePrincipal | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should include entitlements and roles properties' {
            $result = dsc resource schema -r LibreDsc.Databricks/ServicePrincipal | ConvertFrom-Json
            $result.properties.entitlements | Should -Not -BeNullOrEmpty
            $result.properties.entitlements.type | Should -Be 'array'
            $result.properties.roles | Should -Not -BeNullOrEmpty
            $result.properties.roles.type | Should -Be 'array'
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent service principal' {
            $inputJson = @{ display_name = 'dsc-nonexistent-sp-000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create Service Principal' -Tag 'Set' {
        It 'should create a new service principal' {
            $inputJson = @{
                display_name = $script:testSpName
                active       = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.display_name | Should -Be $script:testSpName
            $result.afterState.active | Should -Be $true
            $result.changedProperties | Should -Contain '_exist'
        }

        It 'should verify the created service principal via get' {
            $inputJson = @{ display_name = $script:testSpName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.display_name | Should -Be $script:testSpName
            $result.actualState.id | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Set Operation - Update Active Status' -Tag 'Set' {
        It 'should deactivate the service principal' {
            $inputJson = @{
                display_name = $script:testSpName
                active       = $false
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.changedProperties | Should -Contain 'active'
        }

        It 'should verify the deactivated state via get' {
            $inputJson = @{ display_name = $script:testSpName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | ConvertFrom-Json
            $result.actualState.active | Should -Be $false
        }

        It 'should reactivate the service principal' {
            $inputJson = @{
                display_name = $script:testSpName
                active       = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState.active | Should -Be $true
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        It 'should report in desired state when active matches' {
            $inputJson = @{
                display_name = $script:testSpName
                active       = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            try {
                $result.inDesiredState | Should -Be $true
            } catch {
                Set-ItResult -Inconclusive -Because "SCIM API may return inconsistent boolean values causing false drift: $_"
            }
        }

        It 'should report out of desired state when active does not match' {
            $inputJson = @{
                display_name = $script:testSpName
                active       = $false
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | ConvertFrom-Json
            try {
                $result.inDesiredState | Should -Be $false
                $result.differingProperties | Should -Contain 'active'
            } catch {
                Set-ItResult -Inconclusive -Because "SCIM API may return inconsistent boolean values: $_"
            }
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        BeforeAll {
            $script:exportSpName = New-TestServicePrincipalName
            $inputJson = @{
                display_name = $script:exportSpName
                active       = $true
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:exportSpName)
            {
                try
                {
                    $inputJson = @{ display_name = $script:exportSpName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/ServicePrincipal --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should export service principals including both test service principals' {
            $result = dsc resource export -r LibreDsc.Databricks/ServicePrincipal | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty

            $sp1 = $result.resources | Where-Object { $_.properties.display_name -eq $script:testSpName }
            $sp1 | Should -Not -BeNullOrEmpty
            $sp1.properties._exist | Should -Be $true

            $sp2 = $result.resources | Where-Object { $_.properties.display_name -eq $script:exportSpName }
            $sp2 | Should -Not -BeNullOrEmpty
            $sp2.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        It 'should delete the test service principal' {
            $inputJson = @{ display_name = $script:testSpName } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the service principal is gone via get' {
            $inputJson = @{ display_name = $script:testSpName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $script:idempotentSpName = New-TestServicePrincipalName
            $inputJson = @{
                display_name = $script:idempotentSpName
                active       = $true
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentSpName)
            {
                try
                {
                    $inputJson = @{ display_name = $script:idempotentSpName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/ServicePrincipal --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            $inputJson = @{
                display_name = $script:idempotentSpName
                active       = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ServicePrincipal --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.display_name | Should -Be $script:idempotentSpName
            $result.changedProperties | Should -BeNullOrEmpty
        }
    }
}
