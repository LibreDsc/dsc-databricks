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

Describe 'Databricks SQL Warehouse Resource' -Tag 'Databricks', 'SqlWarehouse' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }

        $script:testWarehouseName = New-TestSqlWarehouseName

        # Configurable via environment variables.
        # DATABRICKS_WAREHOUSE_CLUSTER_SIZE – e.g. 2X-Small (default)
        $script:clusterSize = if ($env:DATABRICKS_WAREHOUSE_CLUSTER_SIZE) { $env:DATABRICKS_WAREHOUSE_CLUSTER_SIZE } else { '2X-Small' }
    }

    AfterAll {
        if ($script:databricksAvailable -and $script:testWarehouseId)
        {
            try
            {
                $inputJson = @{ id = $script:testWarehouseId } | ConvertTo-Json -Compress
                dsc resource delete -r LibreDsc.Databricks/SqlWarehouse --input $inputJson 2>$null | Out-Null
            }
            catch { }
        }
    }

    Context 'Discovery' -Tag 'Discovery' {
        It 'should be found by dsc' {
            $result = dsc resource list LibreDsc.Databricks/SqlWarehouse | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.type | Should -Be 'LibreDsc.Databricks/SqlWarehouse'
        }

        It 'should report correct capabilities' {
            $result = dsc resource list LibreDsc.Databricks/SqlWarehouse | ConvertFrom-Json
            $result.capabilities | Should -Contain 'get'
            $result.capabilities | Should -Contain 'set'
            $result.capabilities | Should -Contain 'delete'
            $result.capabilities | Should -Contain 'export'
        }
    }

    Context 'Schema Validation' -Tag 'Schema' {
        It 'should return valid JSON schema' {
            $result = dsc resource schema -r LibreDsc.Databricks/SqlWarehouse | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.'$schema' | Should -Be 'https://json-schema.org/draft/2020-12/schema'
            $result.properties.name | Should -Not -BeNullOrEmpty
            $result.properties.id | Should -Not -BeNullOrEmpty
            $result.properties.cluster_size | Should -Not -BeNullOrEmpty
            $result.properties.auto_stop_mins | Should -Not -BeNullOrEmpty
        }

        It 'should include _exist property with default true' {
            $result = dsc resource schema -r LibreDsc.Databricks/SqlWarehouse | ConvertFrom-Json
            $result.properties._exist | Should -Not -BeNullOrEmpty
            $result.properties._exist.type | Should -Be 'boolean'
            $result.properties._exist.default | Should -Be $true
        }

        It 'should include warehouse configuration properties' {
            $result = dsc resource schema -r LibreDsc.Databricks/SqlWarehouse | ConvertFrom-Json
            $result.properties.min_num_clusters | Should -Not -BeNullOrEmpty
            $result.properties.max_num_clusters | Should -Not -BeNullOrEmpty
            $result.properties.enable_photon | Should -Not -BeNullOrEmpty
            $result.properties.enable_serverless_compute | Should -Not -BeNullOrEmpty
            $result.properties.spot_instance_policy | Should -Not -BeNullOrEmpty
            $result.properties.warehouse_type | Should -Not -BeNullOrEmpty
            $result.properties.channel | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Get Operation' -Tag 'Get' {
        It 'should return _exist=false for a non-existent warehouse by name' {
            $inputJson = @{ name = 'dsc-nonexistent-warehouse-000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }

        It 'should return _exist=false for a non-existent warehouse by id' {
            $inputJson = @{ id = '0000000000000000' } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Set Operation - Create' -Tag 'Set', 'Create' {
        It 'should create a new SQL warehouse and wait for RUNNING state' {
            $inputJson = @{
                name           = $script:testWarehouseName
                cluster_size   = $script:clusterSize
                auto_stop_mins = 10
                min_num_clusters = 1
                max_num_clusters = 1
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:testWarehouseName
            $result.afterState.id | Should -Not -BeNullOrEmpty
            $result.afterState.state | Should -Be 'RUNNING'
            $result.afterState.cluster_size | Should -Be $script:clusterSize
            $result.changedProperties | Should -Contain '_exist'
            $script:testWarehouseId = $result.afterState.id
        }

        It 'should verify the created warehouse via get' {
            $inputJson = @{ id = $script:testWarehouseId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.name | Should -Be $script:testWarehouseName
            $result.actualState.cluster_size | Should -Be $script:clusterSize
            $result.actualState.auto_stop_mins | Should -Be 10
        }

        It 'should report state as RUNNING after creation' {
            $inputJson = @{ id = $script:testWarehouseId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $result.actualState.state | Should -Be 'RUNNING'
        }
    }

    Context 'Set Operation - Update' -Tag 'Set', 'Update' {
        It 'should update the auto_stop_mins' {
            $inputJson = @{
                id             = $script:testWarehouseId
                name           = $script:testWarehouseName
                cluster_size   = $script:clusterSize
                auto_stop_mins = 20
                min_num_clusters = 1
                max_num_clusters = 1
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.auto_stop_mins | Should -Be 20
            $result.changedProperties | Should -Contain 'auto_stop_mins'
        }

        It 'should verify the update persisted via get' {
            $inputJson = @{ id = $script:testWarehouseId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $result.actualState.auto_stop_mins | Should -Be 20
        }

        It 'should report RUNNING state after edit' {
            $inputJson = @{ id = $script:testWarehouseId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $result.actualState.state | Should -Be 'RUNNING'
        }
    }

    Context 'State Transitions' -Tag 'State' {
        It 'should stop the warehouse via API' {
            $baseUrl = $env:DATABRICKS_HOST.TrimEnd('/')
            $headers = @{ Authorization = "Bearer $env:DATABRICKS_TOKEN"; 'Content-Type' = 'application/json' }
            $body = @{ id = $script:testWarehouseId } | ConvertTo-Json -Compress
            Invoke-RestMethod -Uri "$baseUrl/api/2.0/sql/warehouses/$($script:testWarehouseId)/stop" -Method Post -Headers $headers -Body $body | Out-Null

            # Poll until stopped
            $timeout = [DateTime]::UtcNow.AddMinutes(15)
            do {
                Start-Sleep -Seconds 10
                $resp = Invoke-RestMethod -Uri "$baseUrl/api/2.0/sql/warehouses/$($script:testWarehouseId)" -Headers $headers
            } while ($resp.state -notin @('STOPPED', 'DELETED') -and [DateTime]::UtcNow -lt $timeout)
        }

        It 'should report STOPPED state via get' {
            $inputJson = @{ id = $script:testWarehouseId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $result.actualState.state | Should -Be 'STOPPED'
            $result.actualState._exist | Should -Be $true
        }

        It 'should update a stopped warehouse without restarting it' {
            $inputJson = @{
                id             = $script:testWarehouseId
                name           = $script:testWarehouseName
                cluster_size   = $script:clusterSize
                auto_stop_mins = 30
                min_num_clusters = 1
                max_num_clusters = 1
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.auto_stop_mins | Should -Be 30
            # Warehouse remains STOPPED — edit on stopped warehouse doesn't restart
            $result.afterState.state | Should -Be 'STOPPED'
        }
    }

    Context 'Get by name' -Tag 'Get' {
        It 'should find the warehouse by name' {
            $inputJson = @{ name = $script:testWarehouseName } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $true
            $result.actualState.id | Should -Be $script:testWarehouseId
            $result.actualState.name | Should -Be $script:testWarehouseName
        }
    }

    Context 'Export Operation' -Tag 'Export' {
        It 'should export warehouses and include the test warehouse' {
            $result = dsc resource export -r LibreDsc.Databricks/SqlWarehouse | ConvertFrom-Json
            $result | Should -Not -BeNullOrEmpty
            $result.resources | Should -Not -BeNullOrEmpty

            $w = $result.resources | Where-Object { $_.properties.id -eq $script:testWarehouseId }
            $w | Should -Not -BeNullOrEmpty
            $w.properties._exist | Should -Be $true
            $w.properties.name | Should -Be $script:testWarehouseName
        }

        It 'should include state information in exported warehouses' {
            $result = dsc resource export -r LibreDsc.Databricks/SqlWarehouse | ConvertFrom-Json
            $w = $result.resources | Where-Object { $_.properties.id -eq $script:testWarehouseId }
            $w.properties.state | Should -Not -BeNullOrEmpty
        }
    }

    Context 'Delete Operation' -Tag 'Delete' {
        It 'should delete the warehouse' {
            $inputJson = @{ id = $script:testWarehouseId } | ConvertTo-Json -Compress
            dsc resource delete -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | Out-Null
            $LASTEXITCODE | Should -Be 0
        }

        It 'should confirm the warehouse is gone via get' {
            $inputJson = @{ id = $script:testWarehouseId } | ConvertTo-Json -Compress
            $result = dsc resource get -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $result.actualState._exist | Should -Be $false
        }
    }

    Context 'Idempotency' -Tag 'Idempotency' {
        BeforeAll {
            $script:idempotentWarehouseName = New-TestSqlWarehouseName
            $inputJson = @{
                name           = $script:idempotentWarehouseName
                cluster_size   = $script:clusterSize
                auto_stop_mins = 10
                min_num_clusters = 1
                max_num_clusters = 1
            } | ConvertTo-Json -Compress
            $createResult = dsc resource set -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $script:idempotentWarehouseId = $createResult.afterState.id
        }

        AfterAll {
            if ($script:idempotentWarehouseId)
            {
                try
                {
                    $inputJson = @{ id = $script:idempotentWarehouseId } | ConvertTo-Json -Compress
                    dsc resource delete -r LibreDsc.Databricks/SqlWarehouse --input $inputJson 2>$null | Out-Null
                }
                catch { }
            }
        }

        It 'should be idempotent when set is called with the same desired state' {
            $inputJson = @{
                id             = $script:idempotentWarehouseId
                name           = $script:idempotentWarehouseName
                cluster_size   = $script:clusterSize
                auto_stop_mins = 10
                min_num_clusters = 1
                max_num_clusters = 1
            } | ConvertTo-Json -Compress

            $result = dsc resource set -r LibreDsc.Databricks/SqlWarehouse --input $inputJson | ConvertFrom-Json
            $LASTEXITCODE | Should -Be 0
            $result.afterState._exist | Should -Be $true
            $result.afterState.name | Should -Be $script:idempotentWarehouseName
        }
    }
}
