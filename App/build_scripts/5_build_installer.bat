@echo off
REM ============================================================
REM Step 5: Compile the Inno Setup installer.
REM Requires: Inno Setup installed (https://jrsoftware.org/isinfo.php)
REM ============================================================

set ISS_FILE=..\installer\waves_processor.iss
set ISCC="C:\Program Files (x86)\Inno Setup 6\ISCC.exe"

if not exist %ISCC% (
    echo ERROR: Inno Setup not found at %ISCC%
    echo Download from https://jrsoftware.org/isinfo.php and install it.
    pause
    exit /b 1
)

echo.
echo === Compiling installer ===
%ISCC% "%ISS_FILE%"
if errorlevel 1 (
    echo ERROR: Inno Setup compilation failed.
    pause
    exit /b 1
)

echo.
echo Installer created in installer\Output\
pause
