$envFile = Join-Path $PSScriptRoot '..\docker\dags\.env'
if (-not (Test-Path $envFile)) {
    Write-Error "Env file not found: $envFile"
    exit 1
}

Get-Content $envFile | ForEach-Object {
    if ($_ -match '^\s*([^#;\s].*?)\s*=\s*(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()
        Set-Item -Path "Env:\$name" -Value $value
    }
}

Set-Location $PSScriptRoot
dbt run --profiles-dir $PSScriptRoot
