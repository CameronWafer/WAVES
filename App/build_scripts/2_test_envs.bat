@echo off
REM ============================================================
REM Step 2: Smoke-test both environments before packing.
REM Checks that executables and Java respond.
REM Run from Anaconda Prompt or Miniforge Prompt.
REM ============================================================

echo.
echo === Testing WAVES_actinet ===
call conda activate WAVES_actinet

echo -- actinet --help --
actinet --help
if errorlevel 1 (
    echo ERROR: actinet --help failed.
    pause
    exit /b 1
)

echo -- java -version --
java -version
if errorlevel 1 (
    echo ERROR: java -version failed inside WAVES_actinet.
    pause
    exit /b 1
)

call conda deactivate

echo.
echo === Testing WAVES_accelerometer ===
call conda activate WAVES_accelerometer

echo -- accProcess --help --
accProcess --help
if errorlevel 1 (
    echo ERROR: accProcess --help failed.
    pause
    exit /b 1
)

echo -- java -version --
java -version
if errorlevel 1 (
    echo ERROR: java -version failed inside WAVES_accelerometer.
    pause
    exit /b 1
)

call conda deactivate

echo.
echo Both environments passed smoke tests.
echo IMPORTANT: Now test on a real .gt3x file before packing.
echo See the README for manual test commands.
pause
