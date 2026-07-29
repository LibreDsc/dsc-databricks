[CmdletBinding()]
param (
    [Parameter()]
    [System.String]
    $ExeName = 'dsc-databricks'
)

BeforeDiscovery {
    . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
    $script:databricksAvailable = Initialize-DatabricksTests -ExeName $ExeName
    $script:serviceCredentialTestable = $script:databricksAvailable -and
        (Test-UnityCatalogAvailable) -and
        [bool]$env:DATABRICKS_ACCESS_CONNECTOR_ID
    if ($script:databricksAvailable -and -not $env:DATABRICKS_ACCESS_CONNECTOR_ID) {
        Write-Warning 'DATABRICKS_ACCESS_CONNECTOR_ID is not set. Skipping ServiceCredential tests.'
    }
}

Describe 'Databricks ServiceCredential Resource' -Tag 'Databricks', 'ServiceCredential', 'UnityCatalog' -Skip:(!$script:serviceCredentialTestable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testCredentialName = New-TestServiceCredentialName
        $script:accessConnectorId = $env:DATABRICKS_ACCESS_CONNECTOR_ID
    }

    AfterAll {
        if ($script:testCredentialName)
        {
            try
            {
                $inputJson = @{ name = $script:testCredentialName } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/ServiceCredential --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/ServiceCredential | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/ServiceCredential'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/ServiceCredential | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'test'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
            $result.capabilities | Should -Contain 'setWhatIf'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/ServiceCredential | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.name | Should -Not -BeNullOrEmpty
            $result.properties.purpose | Should -Not -BeNullOrEmpty
            $result.properties.azure_managed_identity | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/ServiceCredential | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should require only name' {
            $result = dsc resource schema -r LibreDsc.Databricks/ServiceCredential | ConvertFrom-Json
            $result.required | Should -Be @('name')
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent service credential' {
            $inputJson = @{ name = 'dsc-nonexistent-servicecred-000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ServiceCredential --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set', 'Create' {
        It 'should create a new service credential with a managed identity' {
            $inputJson = @{
                name                   = $script:testCredentialName
                comment                = 'Created by DSC test'
                azure_managed_identity = @{ access_connector_id = $script:accessConnectorId }
                skip_validation        = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ServiceCredential --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:testCredentialName
            $result.afterState.purpose | Should -Be 'SERVICE'
            $result.afterState.azure_managed_identity.access_connector_id | Should -Be $script:accessConnectorId
            $result.changedProperties | Should -Contain 'name'
            $script:credentialCreated = $true
        }

        It 'should verify the created credential via get' {
            if (-not $script:credentialCreated) { Set-ItResult -Skipped -Because 'the service credential fixture was not created' }
            $inputJson = @{ name = $script:testCredentialName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ServiceCredential --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.id | Should -Not -BeNullOrEmpty
            $result.actualState.comment | Should -Be 'Created by DSC test'
        }
    }

    Context 'Set Operation - Update' -Tag 'Set', 'Update' {
        BeforeEach {
            if (-not $script:credentialCreated) { Set-ItResult -Skipped -Because 'the service credential fixture was not created' }
        }

        It 'should update the comment' {
            $inputJson = @{
                name                   = $script:testCredentialName
                comment                = 'Updated by DSC test'
                azure_managed_identity = @{ access_connector_id = $script:accessConnectorId }
                skip_validation        = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ServiceCredential --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState.comment | Should -Be 'Updated by DSC test'
            $result.changedProperties | Should -Contain 'comment'
        }
    }

    Context 'Test Operation' -Tag 'Test' {
        BeforeEach {
            if (-not $script:credentialCreated) { Set-ItResult -Skipped -Because 'the service credential fixture was not created' }
        }

        It 'should report inDesiredState=true despite server-computed nested fields' {
            $inputJson = @{
                name                   = $script:testCredentialName
                comment                = 'Updated by DSC test'
                azure_managed_identity = @{ access_connector_id = $script:accessConnectorId }
                skip_validation        = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/ServiceCredential --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $true
            $result.differingProperties | Should -BeNullOrEmpty
        }

        It 'should report inDesiredState=false when comment differs' {
            $inputJson = @{
                name    = $script:testCredentialName
                comment = 'Different comment'
            } | ConvertTo-Json -Compress

            $result = dsc resource test -r LibreDsc.Databricks/ServiceCredential --input $inputJson | ConvertFrom-Json
            $result.inDesiredState | Should -Be $false
            $result.differingProperties | Should -Contain 'comment'
        }
    }

    Context 'WhatIf Operation' -Tag 'WhatIf' {
        It 'should predict credential creation without creating anything' {
            $script:whatIfCredentialName = New-TestServiceCredentialName
            $result = Invoke-DscWhatIf -ResourceType 'LibreDsc.Databricks/ServiceCredential' -Properties @{
                name                   = $script:whatIfCredentialName
                azure_managed_identity = @{ access_connector_id = $script:accessConnectorId }
            }
            $LASTEXITCODE | Should -Be 0
            $result.metadata.'Microsoft.DSC'.executionType | Should -Be 'whatIf'
            $result.results[0].result.afterState._exist | Should -Be $true
            $result.results[0].result.afterState.purpose | Should -Be 'SERVICE'
        }

        It 'should not have created the credential' {
            $inputJson = @{ name = $script:whatIfCredentialName } | ConvertTo-Json -Compress
            $get = dsc resource get -r LibreDsc.Databricks/ServiceCredential --input $inputJson | ConvertFrom-Json
            $get.actualState._exist | Should -Be $false
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        BeforeEach {
            if (-not $script:credentialCreated) { Set-ItResult -Skipped -Because 'the service credential fixture was not created' }
        }

        It 'should export service credentials including the test credential' {
            $result = dsc resource export -r LibreDsc.Databricks/ServiceCredential | ConvertFrom-Json
            $result.resources | Should -Not -BeNullOrEmpty

            $c = $result.resources | Where-Object { $_.properties.name -eq $script:testCredentialName }
            $c | Should -Not -BeNullOrEmpty
            $c.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        BeforeEach {
            if (-not $script:credentialCreated) { Set-ItResult -Skipped -Because 'the service credential fixture was not created' }
        }

        It 'should delete the test credential' {
            $inputJson = @{ name = $script:testCredentialName } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/ServiceCredential --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the credential is gone via get' {
            $inputJson = @{ name = $script:testCredentialName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/ServiceCredential --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $script:idempotentCredentialName = New-TestServiceCredentialName
            $inputJson = @{
                name                   = $script:idempotentCredentialName
                comment                = 'Idempotency test credential'
                azure_managed_identity = @{ access_connector_id = $env:DATABRICKS_ACCESS_CONNECTOR_ID }
                skip_validation        = $true
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/ServiceCredential --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentCredentialName)
            {
                try
                {
                    $inputJson = @{ name = $script:idempotentCredentialName } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/ServiceCredential --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            $inputJson = @{
                name                   = $script:idempotentCredentialName
                comment                = 'Idempotency test credential'
                azure_managed_identity = @{ access_connector_id = $env:DATABRICKS_ACCESS_CONNECTOR_ID }
                skip_validation        = $true
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/ServiceCredential --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:idempotentCredentialName
        }
    }
}
