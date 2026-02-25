# TODO: Have to fix up the testing

# [CmdletBinding()]
# param (
#     [Parameter()]
#     [System.String]
#     $ExeName = 'dsc-databricks'
# )

# BeforeDiscovery {
#     . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
#     $script:databricksAvailable = Initialize-DatabricksTests -ExeName $ExeName
# }

# Describe 'Databricks User Resource' -Tag 'Databricks', 'User' -Skip:(!$script:databricksAvailable) {
#     BeforeAll {
#         . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

#         $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
#         if (Test-Path $outputDir) {
#             $env:DSC_RESOURCE_PATH = $outputDir
#         }

#         $script:testUserName = New-TestUserName
#         $script:testDisplayName = 'DSC Test User'
#         $script:testDisplayNameUpdated = 'DSC Test User Updated'
#     }

#     AfterAll {
#         # Cleanup: delete the test user if it exists
#         if ($script:databricksAvailable -and $script:testUserName)
#         {
#             try
#             {
#                 $inputJson = @{ user_name = $script:testUserName } | ConvertTo-Json -Compress
#                 dsc resource delete -r LibreDsc.Databricks/User --input $inputJson 2>$null | Out-Null
#             }
#             catch { }
#         }
#     }

#     Context 'Discovery' -Tag 'Discovery' {
#         It 'should be found by dsc' {
#             $result = dsc resource list LibreDsc.Databricks/User | ConvertFrom-Json
#             $result | Should -Not -BeNullOrEmpty
#             $result.type | Should -Be 'LibreDsc.Databricks/User'
#         }

#         It 'should report correct capabilities' {
#             $result = dsc resource list LibreDsc.Databricks/User | ConvertFrom-Json
#             $result.capabilities | Should -Contain 'get'
#             $result.capabilities | Should -Contain 'set'
#             $result.capabilities | Should -Contain 'delete'
#             $result.capabilities | Should -Contain 'export'
#         }
#     }

#     Context 'Schema Validation' -Tag 'Schema' {
#         It 'should return valid JSON schema' {
#             $result = dsc resource schema -r LibreDsc.Databricks/User | ConvertFrom-Json
#             $result | Should -Not -BeNullOrEmpty
#             $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
#             $result.properties.user_name | Should -Not -BeNullOrEmpty
#             $result.properties.display_name | Should -Not -BeNullOrEmpty
#             $result.properties.active | Should -Not -BeNullOrEmpty
#         }

#         It 'should include _exist property with default true' {
#             $result = dsc resource schema -r LibreDsc.Databricks/User | ConvertFrom-Json
#             $result.properties._exist | Should -Not -BeNullOrEmpty
#             $result.properties._exist.type | Should -Be 'boolean'
#             $result.properties._exist.default | Should -Be $true
#         }

#         It 'should include emails, entitlements, and roles properties' {
#             $result = dsc resource schema -r LibreDsc.Databricks/User | ConvertFrom-Json
#             $result.properties.emails | Should -Not -BeNullOrEmpty
#             $result.properties.emails.type | Should -Be 'array'
#             $result.properties.entitlements | Should -Not -BeNullOrEmpty
#             $result.properties.entitlements.type | Should -Be 'array'
#             $result.properties.roles | Should -Not -BeNullOrEmpty
#             $result.properties.roles.type | Should -Be 'array'
#         }

#         It 'should have enum values on entitlements value field' {
#             $result = dsc resource schema -r LibreDsc.Databricks/User | ConvertFrom-Json
#             $enum = $result.properties.entitlements.items.properties.value.enum
#             $enum | Should -Not -BeNullOrEmpty
#             $enum | Should -Contain 'workspace-access'
#             $enum | Should -Contain 'databricks-sql-access'
#             $enum | Should -Contain 'allow-cluster-create'
#             $enum | Should -Contain 'allow-instance-pool-create'
#             $enum | Should -Contain 'workspace-consume'
#         }
#     }

#     Context 'Get Operation' -Tag 'Get' {
#         It 'should return _exist=false for non-existent user' {
#             $inputJson = @{ user_name = 'nonexistent-user-dsc-test@example.com' } | ConvertTo-Json -Compress
#             $result = dsc resource get -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
#             $result.actualState._exist | Should -Be $false
#         }
#     }

#     Context 'Set Operation - Create User' -Tag 'Set' {
#         It 'should create a new user' {
#             $inputJson = @{
#                 user_name    = $script:testUserName
#                 display_name = $script:testDisplayName
#                 active       = $true
#             } | ConvertTo-Json -Compress

#             $result = dsc resource set -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
#             $LASTEXITCODE | Should -Be 0
#             $result.afterState._exist | Should -Be $true
#             $result.afterState.user_name | Should -Be $script:testUserName
#         }

#         It 'should verify the created user via get' {
#             $inputJson = @{ user_name = $script:testUserName } | ConvertTo-Json -Compress
#             $result = dsc resource get -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
#             $result.actualState._exist | Should -Be $true
#             $result.actualState.user_name | Should -Be $script:testUserName
#             $result.actualState.display_name | Should -Be $script:testDisplayName
#         }
#     }

#     Context 'Set Operation - Update Entitlements and Emails' -Tag 'Set' {
#         BeforeAll {
#             $script:entitlementUser = New-TestUserName
#             $inputJson = @{
#                 user_name    = $script:entitlementUser
#                 display_name = 'Entitlement Test'
#                 active       = $true
#             } | ConvertTo-Json -Compress
#             dsc resource set -r LibreDsc.Databricks/User --input $inputJson | Out-Null
#         }

#         AfterAll {
#             if ($script:entitlementUser)
#             {
#                 try
#                 {
#                     $inputJson = @{ user_name = $script:entitlementUser } | ConvertTo-Json -Compress
#                     dsc resource delete -r LibreDsc.Databricks/User --input $inputJson 2>$null | Out-Null
#                 }
#                 catch { }
#             }
#         }

#         It 'should set entitlements and emails on the user' {
#             $inputJson = @{
#                 user_name    = $script:entitlementUser
#                 display_name = 'Entitlement Test'
#                 active       = $true
#                 entitlements = @(
#                     @{ value = 'allow-cluster-create' }
#                 )
#                 emails       = @(
#                     @{ value = $script:entitlementUser; type = 'work'; primary = $true }
#                 )
#             } | ConvertTo-Json -Compress -Depth 5

#             $result = dsc resource set -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
#             $LASTEXITCODE | Should -Be 0
#             $result.afterState._exist | Should -Be $true
#             $result.afterState.user_name | Should -Be $script:entitlementUser
#         }

#         It 'should verify entitlements and emails via get' {
#             $inputJson = @{ user_name = $script:entitlementUser } | ConvertTo-Json -Compress
#             $result = dsc resource get -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
#             $result.actualState._exist | Should -Be $true
#             $result.actualState.entitlements | Should -Not -BeNullOrEmpty
#             ($result.actualState.entitlements | Where-Object { $_.value -eq 'allow-cluster-create' }) | Should -Not -BeNullOrEmpty
#             $result.actualState.emails | Should -Not -BeNullOrEmpty
#             ($result.actualState.emails | Where-Object { $_.value -eq $script:entitlementUser }) | Should -Not -BeNullOrEmpty
#         }
#     }

#     Context 'Set Operation - Update User' -Tag 'Set' {
#         It 'should update active status' {
#             $inputJson = @{
#                 user_name    = $script:testUserName
#                 display_name = $script:testDisplayName
#                 active       = $false
#             } | ConvertTo-Json -Compress

#             $result = dsc resource set -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
#             $LASTEXITCODE | Should -Be 0

#             # Verify the update
#             $verifyJson = @{ user_name = $script:testUserName } | ConvertTo-Json -Compress
#             $verify = dsc resource get -r LibreDsc.Databricks/User --input $verifyJson | ConvertFrom-Json
#             $verify.actualState.active | Should -Be $false
#         }
#     }

#     Context 'Test Operation' -Tag 'Test' {
#         It 'should report in desired state when matching' {
#             $inputJson = @{
#                 user_name    = $script:testUserName
#                 display_name = $script:testDisplayName
#                 active       = $false
#             } | ConvertTo-Json -Compress

#             $result = dsc resource test -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
#             $result.inDesiredState | Should -Be $true
#             $result.differingProperties | Should -BeNullOrEmpty
#         }

#         It 'should report not in desired state when active status differs' {
#             $inputJson = @{
#                 user_name    = $script:testUserName
#                 display_name = $script:testDisplayName
#                 active       = $true
#             } | ConvertTo-Json -Compress

#             $result = dsc resource test -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
#             $result.inDesiredState | Should -Be $false
#             $result.differingProperties | Should -Contain 'active'
#         }
#     }

#     Context 'Export Operation' -Tag 'Export' {
#         It 'should export users with resources' {
#             $result = dsc resource export -r LibreDsc.Databricks/User | ConvertFrom-Json
#             $result | Should -Not -BeNullOrEmpty
#             $result.resources | Should -Not -BeNullOrEmpty
#             $result.resources.Count | Should -BeGreaterOrEqual 1
#         }
#     }

#     Context 'Delete Operation' -Tag 'Delete' {
#         It 'should delete the test user' {
#             $inputJson = @{ user_name = $script:testUserName } | ConvertTo-Json -Compress
#             dsc resource delete -r LibreDsc.Databricks/User --input $inputJson | Out-Null
#             $LASTEXITCODE | Should -Be 0

#             Start-Sleep -Seconds 2 # Wait for eventual consistency
#             # Verify deletion
#             $result = dsc resource get -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
#             $result.actualState._exist | Should -Be $false
#         }
#     }

#     Context 'Idempotency' -Tag 'Idempotency' {
#         BeforeAll {
#             # Create a user for idempotency testing
#             $script:idempotentUser = New-TestUserName
#             $inputJson = @{
#                 user_name    = $script:idempotentUser
#                 display_name = 'Idempotent Test'
#                 active       = $true
#             } | ConvertTo-Json -Compress
#             dsc resource set -r LibreDsc.Databricks/User --input $inputJson | Out-Null
#         }

#         AfterAll {
#             if ($script:idempotentUser)
#             {
#                 try
#                 {
#                     $inputJson = @{ user_name = $script:idempotentUser } | ConvertTo-Json -Compress
#                     dsc resource delete -r LibreDsc.Databricks/User --input $inputJson 2>$null | Out-Null
#                 }
#                 catch { }
#             }
#         }

#         It 'should be idempotent when setting the same user twice' {
#             $inputJson = @{
#                 user_name    = $script:idempotentUser
#                 display_name = 'Idempotent Test'
#                 active       = $true
#             } | ConvertTo-Json -Compress

#             dsc resource set -r LibreDsc.Databricks/User --input $inputJson | Out-Null
#             $LASTEXITCODE | Should -Be 0

#             dsc resource set -r LibreDsc.Databricks/User --input $inputJson | Out-Null
#             $LASTEXITCODE | Should -Be 0

#             $verifyJson = @{ user_name = $script:idempotentUser } | ConvertTo-Json -Compress
#             $result = dsc resource get -r LibreDsc.Databricks/User --input $verifyJson | ConvertFrom-Json
#             $result.actualState._exist | Should -Be $true
#             $result.actualState.display_name | Should -Be 'Idempotent Test'
#         }
#     }
# }
