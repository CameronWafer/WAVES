@echo off
REM ============================================================
REM Step 1: Build Conda environments from YAML definitions.
REM Run this from Anaconda Prompt or Miniforge Prompt as the
REM developer. The user never runs this.
REM ============================================================

echo.
echo === Building WAVES_actinet environment ===
call conda env create -f ..\build_envs\actinet_environment.yml
if errorlevel 1 (
    echo.
    echo ERROR: Failed to create WAVES_actinet environment.
    echo Check the error above and verify actinet==0.7.0 is available on PyPI.
    pause
    exit /b 1
)

echo.
echo === Building WAVES_accelerometer environment ===
call conda env create -f ..\build_envs\accelerometer_environment.yml
if errorlevel 1 (
    echo.
    echo ERROR: Failed to create WAVES_accelerometer environment.
    echo Check the error above and verify accelerometer==7.2.3 is available on PyPI.
    pause
    exit /b 1
)

echo.
echo === Verifying environments ===
call conda env list

echo.
echo Done. Both environments created.
echo Next: run 2_test_envs.bat to verify they work before packing.
pause
