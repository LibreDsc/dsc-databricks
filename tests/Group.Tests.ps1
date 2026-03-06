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

Describe 'Databricks Group Resource' -Tag 'Databricks', 'Group' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testGroupName = New-TestGroupName
    }

    AfterAll {
        if ($script:databricksAvailable -and $script:testGroupName)
        {
            try
            {
                $inputJson = @{ display_name = $script:testGroupName } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/Group --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/Group | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/Group'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/Group | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/Group | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.display_name | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/Group | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should include entitlements, members, and roles properties' {
            $result = dsc resource schema -r LibreDsc.Databricks/Group | ConvertFrom-Json
            $result.properties.entitlements | Should -Not -BeNullOrEmpty
            $result.properties.entitlements.type | Should -Be 'array'
            $result.properties.members | Should -Not -BeNullOrEmpty
            $result.properties.members.type | Should -Be 'array'
            $result.properties.roles | Should -Not -BeNullOrEmpty
            $result.properties.roles.type | Should -Be 'array'
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent group' {
            $inputJson = @{ display_name = 'dsc-nonexistent-group-000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Group --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create Group' -Tag 'Set' {
        It 'should create a new group' {
            $inputJson = @{
                display_name = $script:testGroupName
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Group --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.display_name | Should -Be $script:testGroupName
            $result.changedProperties | Should -Contain '_exist'
            $script:testGroupId = $result.afterState.id
        }

        It 'should verify the created group via get' {
            $inputJson = @{ display_name = $script:testGroupName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Group --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.display_name | Should -Be $script:testGroupName
            $result.actualState.id | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Set Operation - Update Group' -Tag 'Set' {
        It 'should update the display_name of the group' {
            $newName = "$($script:testGroupName)-updated"

            $inputJson = @{
                id           = $script:testGroupId
                display_name = $newName
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Group --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.display_name | Should -Be $newName
            $result.changedProperties | Should -Contain 'display_name'
            $script:testGroupName = $newName
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        It 'should report in desired state when display_name matches' {
            $inputJson = @{
                id           = $script:testGroupId
                display_name = $script:testGroupName
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Group --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.inDesiredState | Should -Be $true
        }

        It 'should report out of desired state when display_name does not match' {
            $inputJson = @{
                id           = $script:testGroupId
                display_name = 'wrong-display-name'
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/Group --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $false
            $result.differingProperties | Should -Contain 'display_name'
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        BeforeAll {
            $script:exportGroupName = New-TestGroupName
            $inputJson = @{
                display_name = $script:exportGroupName
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/Group --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:exportGroupName)
            {
                try
                {
                    $inputJson = @{ display_name = $script:exportGroupName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/Group --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should export groups including both test groups' {
            $result = dsc resource export -r LibreDsc.Databricks/Group | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty

            $g1 = $result.resources | Where-Object { $_.properties.display_name -eq $script:testGroupName }
            $g1 | Should -Not -BeNullOrEmpty
            $g1.properties._exist | Should -Be $true

            $g2 = $result.resources | Where-Object { $_.properties.display_name -eq $script:exportGroupName }
            $g2 | Should -Not -BeNullOrEmpty
            $g2.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        It 'should delete the test group' {
            $inputJson = @{ display_name = $script:testGroupName } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/Group --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the group is gone via get' {
            $inputJson = @{ display_name = $script:testGroupName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Group --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $script:idempotentGroupName = New-TestGroupName
            $inputJson = @{
                display_name = $script:idempotentGroupName
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/Group --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentGroupName)
            {
                try
                {
                    $inputJson = @{ display_name = $script:idempotentGroupName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/Group --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            $inputJson = @{
                display_name = $script:idempotentGroupName
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Group --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.display_name | Should -Be $script:idempotentGroupName
            $result.changedProperties | Should -BeNullOrEmpty
        }
    }
}
