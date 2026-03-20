[CmdletBinding()]
param (
    [Parameter()]
    [System.String]
    $ExeName = 'dsc-databricks'
)

BeforeDiscovery {
    . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
    $script:databricksAvailable = Initialize-DatabricksTests -ExeName $ExeName
    $script:accountAvailable = $script:databricksAvailable -and $env:DATABRICKS_ACCOUNT_ID
}

Describe 'Databricks AccountUser Resource' -Tag 'Databricks', 'AccountUser' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testUserName = New-TestAccountUserName
        $script:testDisplayName = 'DSC Test Account User'
        $script:testDisplayNameUpdated = 'DSC Test Account User Updated'
    }

    AfterAll {
        if ($script:databricksAvailable -and $script:testUserName)
        {
            try
            {
                $inputJson = @{ user_name = $script:testUserName } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/AccountUser --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/AccountUser | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/AccountUser'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/AccountUser | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/AccountUser | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.user_name | Should -Not -BeNullOrEmpty
            $result.properties.display_name | Should -Not -BeNullOrEmpty
            $result.properties.active | Should -Not -BeNullOrEmpty
            $result.properties.emails | Should -Not -BeNullOrEmpty
            $result.properties.roles | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/AccountUser | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should require user_name' {
            $result = dsc resource schema -r LibreDsc.Databricks/AccountUser | ConvertFrom-Json
            $result.required | Should -Contain 'user_name'
        }
    }

    Context 'Get Operation' -Tag 'Get' -Skip:(!$script:accountAvailable) {
        It 'should return _exist=false for a non-existent account user' {
            $inputJson = @{ user_name = 'dsc-nonexistent-acctuser-000@example.com' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/AccountUser --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set' -Skip:(!$script:accountAvailable) {
        It 'should create a new account user' {
            $inputJson = @{
                user_name    = $script:testUserName
                display_name = $script:testDisplayName
                active       = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/AccountUser --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.user_name | Should -Be $script:testUserName
            $result.afterState.display_name | Should -Be $script:testDisplayName
            $result.changedProperties | Should -Contain '_exist'
        }

        It 'should verify the created account user via get' {
            $inputJson = @{ user_name = $script:testUserName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/AccountUser --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.user_name | Should -Be $script:testUserName
            $result.actualState.display_name | Should -Be $script:testDisplayName
        }
    }

    Context 'Set Operation - Update' -Tag 'Set' -Skip:(!$script:accountAvailable) {
        It 'should update the display name of the account user' {
            $inputJson = @{
                user_name    = $script:testUserName
                display_name = $script:testDisplayNameUpdated
                active       = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/AccountUser --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.display_name | Should -Be $script:testDisplayNameUpdated
            $result.changedProperties | Should -Contain 'display_name'
        }

        It 'should verify the update via get' {
            $inputJson = @{ user_name = $script:testUserName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/AccountUser --input $inputJson | ConvertFrom-Json
            $result.actualState.display_name | Should -Be $script:testDisplayNameUpdated
        }
    }

    Context 'Test Operation' -Tag 'Test' -Skip:(!$script:accountAvailable) {
        It 'should report inDesiredState=true when state matches' {
            $inputJson = @{
                user_name    = $script:testUserName
                display_name = $script:testDisplayNameUpdated
                active       = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/AccountUser --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $true
            $result.differingProperties | Should -BeNullOrEmpty
        }

        It 'should report inDesiredState=false when display_name differs' {
            $inputJson = @{
                user_name    = $script:testUserName
                display_name = 'Wrong Name'
                active       = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/AccountUser --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $false
            $result.differingProperties | Should -Contain 'display_name'
        }
    }

    Context 'Export Operation' -Tag 'Export' -Skip:(!$script:accountAvailable) {
        It 'should export account users including the test user' {
            $result = dsc resource export -r LibreDsc.Databricks/AccountUser | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty

            $u = $result.resources | Where-Object { $_.properties.user_name -eq $script:testUserName }
            $u | Should -Not -BeNullOrEmpty
            $u.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' -Skip:(!$script:accountAvailable) {
        It 'should delete the test account user' {
            $inputJson = @{ user_name = $script:testUserName } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/AccountUser --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the account user is gone via get' {
            $inputJson = @{ user_name = $script:testUserName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/AccountUser --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' -Skip:(!$script:accountAvailable) {
        BeforeAll {
            $script:idempotentUserName = New-TestAccountUserName
            $inputJson = @{
                user_name    = $script:idempotentUserName
                display_name = 'Idempotency test account user'
                active       = $true
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/AccountUser --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentUserName)
            {
                try
                {
                    $inputJson = @{ user_name = $script:idempotentUserName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/AccountUser --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            $inputJson = @{
                user_name    = $script:idempotentUserName
                display_name = 'Idempotency test account user'
                active       = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/AccountUser --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.user_name | Should -Be $script:idempotentUserName
            $result.changedProperties | Should -BeNullOrEmpty
        }
    }
}
