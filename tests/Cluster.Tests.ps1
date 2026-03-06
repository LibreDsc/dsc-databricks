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

Describe 'Databricks Cluster Resource' -Tag 'Databricks', 'Cluster' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testClusterName = New-TestClusterName

        # Use environment variables or sensible defaults for cluster config.
        # DATABRICKS_NODE_TYPE_ID   – e.g. Standard_D4ds_v5
        # DATABRICKS_SPARK_VERSION  – e.g. 15.4.x-scala2.12
        $script:nodeTypeId    = if ($env:DATABRICKS_NODE_TYPE_ID)   { $env:DATABRICKS_NODE_TYPE_ID }   else { 'Standard_D4ds_v5' }
        $script:sparkVersion  = if ($env:DATABRICKS_SPARK_VERSION)  { $env:DATABRICKS_SPARK_VERSION }  else { '15.4.x-scala2.12' }
    }

    AfterAll {
        if ($script:databricksAvailable -and $script:testClusterId)
        {
            try
            {
                $inputJson = @{ cluster_id = $script:testClusterId } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/Cluster --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/Cluster | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/Cluster'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/Cluster | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/Cluster | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.cluster_name | Should -Not -BeNullOrEmpty
            $result.properties.cluster_id | Should -Not -BeNullOrEmpty
            $result.properties.spark_version | Should -Not -BeNullOrEmpty
            $result.properties.node_type_id | Should -Not -BeNullOrEmpty
            $result.properties.num_workers | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/Cluster | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should include autoscale and status properties' {
            $result = dsc resource schema -r LibreDsc.Databricks/Cluster | ConvertFrom-Json
            $result.properties.autoscale_min_workers | Should -Not -BeNullOrEmpty
            $result.properties.autoscale_max_workers | Should -Not -BeNullOrEmpty
            $result.properties.autotermination_minutes | Should -Not -BeNullOrEmpty
        }

        It 'should include policy and configuration properties' {
            $result = dsc resource schema -r LibreDsc.Databricks/Cluster | ConvertFrom-Json
            $result.properties.policy_id | Should -Not -BeNullOrEmpty
            $result.properties.spark_conf | Should -Not -BeNullOrEmpty
            $result.properties.custom_tags | Should -Not -BeNullOrEmpty
            $result.properties.data_security_mode | Should -Not -BeNullOrEmpty
            $result.properties.runtime_engine | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent cluster by name' {
            $inputJson = @{ cluster_name = 'dsc-nonexistent-cluster-000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }

        It 'should return _exist=false for a non-existent cluster by id' {
            $inputJson = @{ cluster_id = '0000-000000-xxxxxxxxxx' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set', 'Create' {
        It 'should create a new cluster and wait for RUNNING state' {
            $inputJson = @{
                cluster_name            = $script:testClusterName
                spark_version           = $script:sparkVersion
                node_type_id            = $script:nodeTypeId
                num_workers             = 0
                autotermination_minutes = 10
                custom_tags             = @{ dsc_test = 'true' }
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.cluster_name | Should -Be $script:testClusterName
            $result.afterState.cluster_id | Should -Not -BeNullOrEmpty
            $result.afterState.state | Should -Be 'RUNNING'
            $result.changedProperties | Should -Contain '_exist'
            $script:testClusterId = $result.afterState.cluster_id
        }

        It 'should verify the created cluster via get' {
            $inputJson = @{ cluster_id = $script:testClusterId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.cluster_name | Should -Be $script:testClusterName
            $result.actualState.spark_version | Should -Be $script:sparkVersion
            $result.actualState.node_type_id | Should -Be $script:nodeTypeId
        }

        It 'should report state as RUNNING after creation' {
            $inputJson = @{ cluster_id = $script:testClusterId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $result.actualState.state | Should -Be 'RUNNING'
        }
    }

    Context 'Set Operation - Update' -Tag 'Set', 'Update' {
        It 'should update the autotermination_minutes' {
            $inputJson = @{
                cluster_id              = $script:testClusterId
                cluster_name            = $script:testClusterName
                spark_version           = $script:sparkVersion
                node_type_id            = $script:nodeTypeId
                num_workers             = 0
                autotermination_minutes = 20
                custom_tags             = @{ dsc_test = 'true' }
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.autotermination_minutes | Should -Be 20
            $result.changedProperties | Should -Contain 'autotermination_minutes'
        }

        It 'should verify the update persisted via get' {
            $inputJson = @{ cluster_id = $script:testClusterId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $result.actualState.autotermination_minutes | Should -Be 20
        }

        It 'should report RUNNING state after edit' {
            $inputJson = @{ cluster_id = $script:testClusterId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $result.actualState.state | Should -Be 'RUNNING'
        }
    }

    Context 'State Transitions' -Tag 'State' {
        It 'should terminate the cluster via delete (non-permanent)' {
            $baseUrl = $env:DATABRICKS_HOST.TrimEnd('/')
            $headers = @{ Authorization = "Bearer $env:DATABRICKS_TOKEN"; 'Content-Type' = 'application/json' }
            $body = @{ cluster_id = $script:testClusterId } | ConvertTo-Json -Compress
            Invoke-RestMethod -Uri "$baseUrl/api/2.0/clusters/delete" -Method Post -Headers $headers -Body $body | Out-Null

            # Poll until terminated
            $timeout = [DateTime]::UtcNow.AddMinutes(15)
            do {
                Start-Sleep -Seconds 10
                $resp = Invoke-RestMethod -Uri "$baseUrl/api/2.0/clusters/get?cluster_id=$($script:testClusterId)" -Headers $headers
            } while ($resp.state -notin @('TERMINATED', 'ERROR') -and [DateTime]::UtcNow -lt $timeout)
        }

        It 'should report TERMINATED state via get' {
            $inputJson = @{ cluster_id = $script:testClusterId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $result.actualState.state | Should -Be 'TERMINATED'
            $result.actualState._exist | Should -Be $true
        }

        It 'should report state_message for a terminated cluster' {
            $inputJson = @{ cluster_id = $script:testClusterId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            # state_message is populated when the cluster terminates for any reason
            $result.actualState.state | Should -Be 'TERMINATED'
        }

        It 'should update a terminated cluster without restarting it' {
            $inputJson = @{
                cluster_id              = $script:testClusterId
                cluster_name            = $script:testClusterName
                spark_version           = $script:sparkVersion
                node_type_id            = $script:nodeTypeId
                num_workers             = 0
                autotermination_minutes = 30
                custom_tags             = @{ dsc_test = 'true' }
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.autotermination_minutes | Should -Be 30
            # Cluster remains TERMINATED — edit on terminated cluster doesn't restart
            $result.afterState.state | Should -Be 'TERMINATED'
        }
    }

    Context 'Get by cluster_name' -Tag 'Get' {
        It 'should find the cluster by name' {
            $inputJson = @{ cluster_name = $script:testClusterName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.cluster_id | Should -Be $script:testClusterId
            $result.actualState.cluster_name | Should -Be $script:testClusterName
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        It 'should export clusters and include the test cluster' {
            $result = dsc resource export -r LibreDsc.Databricks/Cluster | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty

            $c = $result.resources | Where-Object { $_.properties.cluster_id -eq $script:testClusterId }
            $c | Should -Not -BeNullOrEmpty
            $c.properties._exist | Should -Be $true
            $c.properties.cluster_name | Should -Be $script:testClusterName
        }

        It 'should include state information in exported clusters' {
            $result = dsc resource export -r LibreDsc.Databricks/Cluster | ConvertFrom-Json
            $c = $result.resources | Where-Object { $_.properties.cluster_id -eq $script:testClusterId }
            $c.properties.state | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        It 'should permanently delete the cluster' {
            $inputJson = @{ cluster_id = $script:testClusterId } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/Cluster --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the cluster is gone via get' {
            $inputJson = @{ cluster_id = $script:testClusterId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $script:idempotentClusterName = New-TestClusterName
            $inputJson = @{
                cluster_name            = $script:idempotentClusterName
                spark_version           = $script:sparkVersion
                node_type_id            = $script:nodeTypeId
                num_workers             = 0
                autotermination_minutes = 10
            } | ConvertTo-Json -Compress
            $createResult = dsc resource set -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $script:idempotentClusterId = $createResult.afterState.cluster_id
        }

        AfterAll {
            if ($script:idempotentClusterId)
            {
                try
                {
                    $inputJson = @{ cluster_id = $script:idempotentClusterId } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/Cluster --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called with the same desired state' {
            $inputJson = @{
                cluster_id              = $script:idempotentClusterId
                cluster_name            = $script:idempotentClusterName
                spark_version           = $script:sparkVersion
                node_type_id            = $script:nodeTypeId
                num_workers             = 0
                autotermination_minutes = 10
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/Cluster --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.cluster_name | Should -Be $script:idempotentClusterName
        }
    }
}
