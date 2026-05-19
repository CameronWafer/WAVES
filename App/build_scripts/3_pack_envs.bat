@echo off
REM ============================================================
REM Step 3: Pack environments with conda-pack and unzip them
REM into release_build\WAVES Processor\envs\.
REM
REM Requires: conda install -n base conda-pack
REM Run from Anaconda Prompt or Miniforge Prompt.
REM ============================================================

set RELEASE_DIR=..\release_build\WAVES Processor
set ENVS_DIR=%RELEASE_DIR%\envs

echo.
echo === Creating release folder structure ===
if not exist "%ENVS_DIR%" mkdir "%ENVS_DIR%"

echo.
echo === Packing WAVES_actinet ===
call conda activate base
conda-pack -n WAVES_actinet -o "%ENVS_DIR%\WAVES_actinet.zip" --force
if errorlevel 1 (
    echo ERROR: conda-pack failed for WAVES_actinet.
    pause
    exit /b 1
)

echo.
echo === Packing WAVES_accelerometer ===
conda-pack -n WAVES_accelerometer -o "%ENVS_DIR%\WAVES_accelerometer.zip" --force
if errorlevel 1 (
    echo ERROR: conda-pack failed for WAVES_accelerometer.
    pause
    exit /b 1
)

echo.
echo === Unpacking WAVES_actinet ===
powershell -command "Expand-Archive -Path '%ENVS_DIR%\WAVES_actinet.zip' -DestinationPath '%ENVS_DIR%\WAVES_actinet' -Force"

echo.
echo === Unpacking WAVES_accelerometer ===
powershell -command "Expand-Archive -Path '%ENVS_DIR%\WAVES_accelerometer.zip' -DestinationPath '%ENVS_DIR%\WAVES_accelerometer' -Force"

echo.
echo === Running conda-unpack to fix hardcoded paths ===
call "%ENVS_DIR%\WAVES_actinet\Scripts\conda-unpack.exe"
if errorlevel 1 (
    echo ERROR: conda-unpack failed for WAVES_actinet.
    pause
    exit /b 1
)

call "%ENVS_DIR%\WAVES_accelerometer\Scripts\conda-unpack.exe"
if errorlevel 1 (
    echo ERROR: conda-unpack failed for WAVES_accelerometer.
    pause
    exit /b 1
)

echo.
echo === Cleaning up zip files ===
del "%ENVS_DIR%\WAVES_actinet.zip"
del "%ENVS_DIR%\WAVES_accelerometer.zip"

echo.
echo Environments packed and unpacked successfully.
echo Location: %ENVS_DIR%
echo.
echo Next: test the unpacked envs directly (without conda activate):
echo   "%ENVS_DIR%\WAVES_actinet\Scripts\actinet.exe" --help
echo   "%ENVS_DIR%\WAVES_accelerometer\Scripts\accProcess.exe" --help
echo.
echo Then run 4_build_app.bat
pause
