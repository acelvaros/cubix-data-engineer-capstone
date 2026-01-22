param(
    [string]$ProfileName
)

# Configuration
$dbfsPath = "dbfs:/mnt/packages"
$distPath = "$PSScriptRoot\..\dist"

# Step 1: Change to the directory where the script is located (project root)
Write-Output "Changing to the project root directory..."
Set-Location -Path $PSScriptRoot

# Step 2: Build the wheel using Poetry
Write-Output "Building wheel using Poetry..."
poetry build -f wheel
if ($LASTEXITCODE -ne 0) {
    Write-Output "Failed to build the wheel. Exiting."
    exit 1
}
Write-Output "Wheel build complete."

# Prepare Databricks profile arguments if provided
$profileArgs = @()
if ($ProfileName) {
    $profileArgs = @("--profile", $ProfileName)
    Write-Output "Using Databricks profile: $ProfileName"
}

# Step 3: Clear the DBFS packages folder
Write-Output "Truncating $dbfsPath..."
databricks fs rm -r $dbfsPath @profileArgs
if ($LASTEXITCODE -ne 0) {
    Write-Output "Failed to truncate DBFS path. Exiting."
    exit 1
}

databricks fs mkdirs $dbfsPath @profileArgs
if ($LASTEXITCODE -ne 0) {
    Write-Output "Failed to create DBFS directory. Exiting."
    exit 1
}
Write-Output "Truncated $dbfsPath."

# Step 4: Find the latest .whl file in the dist folder
Write-Output "Finding the latest .whl file in $distPath..."
$latestWhl = Get-ChildItem -Path $distPath -Filter *.whl | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $latestWhl) {
    Write-Output "No .whl files found in $distPath. Exiting."
    exit 1
}
Write-Output "Latest .whl file found: $($latestWhl.FullName)"

# Step 5: Copy the latest .whl file to DBFS
Write-Output "Copying $($latestWhl.FullName) to $dbfsPath..."
databricks fs cp $latestWhl.FullName "$dbfsPath/" --overwrite @profileArgs
if ($LASTEXITCODE -ne 0) {
    Write-Output "Failed to copy the .whl file to DBFS. Exiting."
    exit 1
}
Write-Output "Copied $($latestWhl.FullName) to $dbfsPath."

# Completion message
Write-Output "Latest .whl file built and uploaded to $dbfsPath successfully."
