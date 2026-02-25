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

Describe 'Databricks Repo Resource' -Tag 'Databricks', 'Repo' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testRepoPath    = New-TestRepoPath
        # octocat/Hello-World is a stable, minimal public GitHub repo used for testing.
        # It has a default branch 'master' and a 'test' branch.
        $script:testRepoUrl      = 'https://github.com/octocat/Hello-World'
        $script:testRepoProvider = 'gitHub'
        $script:testRepoBranch   = 'master'
        $script:testRepoBranchAlt = 'test'
    }

    AfterAll {
        if ($script:databricksAvailable -and $script:testRepoPath)
        {
            try
            {
                $inputJson = @{ path = $script:testRepoPath } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/Repo --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/Repo | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/Repo'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/Repo | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/Repo | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.path | Should -Not -BeNullOrEmpty
            $result.properties.url | Should -Not -BeNullOrEmpty
            $result.properties.provider | Should -Not -BeNullOrEmpty
            $result.properties.branch | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/Repo | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent repo path' {
            $parent = Get-DatabricksRepoParent
            $inputJson = @{ path = "$parent/dsc-nonexistent-repo-000" } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Repo --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create Repo' -Tag 'Set' {
        It 'should clone the remote repo into the workspace' {
            $inputJson = @{
                path     = $script:testRepoPath
                url      = $script:testRepoUrl
                provider = $script:testRepoProvider
                branch   = $script:testRepoBranch
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Repo --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.path | Should -Be $script:testRepoPath
            $result.afterState.url | Should -Be $script:testRepoUrl
            $result.changedProperties | Should -Contain '_exist'
        }

        It 'should verify the cloned repo via get' {
            $inputJson = @{ path = $script:testRepoPath } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Repo --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.path | Should -Be $script:testRepoPath
            $result.actualState.url | Should -Be $script:testRepoUrl
            $result.actualState.branch | Should -Be $script:testRepoBranch
            $result.actualState.id | Should -BeGreaterThan 0
            $result.actualState.head_commit_id | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Set Operation - Update Branch' -Tag 'Set' {
        It 'should switch to a different branch' {
            $inputJson = @{
                path   = $script:testRepoPath
                branch = $script:testRepoBranchAlt
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Repo --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.branch | Should -Be $script:testRepoBranchAlt
            $result.changedProperties | Should -Contain 'branch'
        }

        It 'should verify the branch change via get' {
            $inputJson = @{ path = $script:testRepoPath } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Repo --input $inputJson | ConvertFrom-Json
            $result.actualState.branch | Should -Be $script:testRepoBranchAlt
        }

        It 'should restore original branch' {
            $inputJson = @{
                path   = $script:testRepoPath
                branch = $script:testRepoBranch
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Repo --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState.branch | Should -Be $script:testRepoBranch
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        It 'should export repos including the test repo' {
            $result = dsc resource export -r LibreDsc.Databricks/Repo | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty
            $testRepo = $result.resources | Where-Object { $_.properties.path -eq $script:testRepoPath }
            $testRepo | Should -Not -BeNullOrEmpty
            $testRepo.properties._exist | Should -Be $true
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        It 'should delete the test repo' {
            $inputJson = @{ path = $script:testRepoPath } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/Repo --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the repo is gone via get' {
            $inputJson = @{ path = $script:testRepoPath } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Repo --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $script:idempotentRepoPath = New-TestRepoPath
            $inputJson = @{
                path     = $script:idempotentRepoPath
                url      = $script:testRepoUrl
                provider = $script:testRepoProvider
                branch   = $script:testRepoBranch
            } | ConvertTo-Json -Compress
            dsc resource set -r LibreDsc.Databricks/Repo --input $inputJson | Out-Null
        }

        AfterAll {
            if ($script:idempotentRepoPath)
            {
                try
                {
                    $inputJson = @{ path = $script:idempotentRepoPath } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/Repo --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called again with the same desired state' {
            $inputJson = @{
                path   = $script:idempotentRepoPath
                branch = $script:testRepoBranch
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Repo --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.branch | Should -Be $script:testRepoBranch
            $result.changedProperties | Should -BeNullOrEmpty
        }
    }
}
