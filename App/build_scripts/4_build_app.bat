@echo off
REM ============================================================
REM Step 4: Build the WAVES Processor exe with PyInstaller.
REM Then copy config, resources, and packed envs into dist/.
REM
REM Requires: pip install pyinstaller  (in base or a build env)
REM Run from a normal command prompt or Anaconda Prompt.
REM ============================================================

set APP_NAME=WAVES Processor
set DIST_DIR=..\dist\%APP_NAME%
set RELEASE_ENVS=..\release_build\WAVES Processor\envs

echo.
echo === Building with PyInstaller ===
cd ..
pyinstaller --windowed --name "%APP_NAME%" --icon resources\app_icon.ico src\waves_gui.py
if errorlevel 1 (
    echo ERROR: PyInstaller build failed.
    pause
    exit /b 1
)

echo.
echo === Copying config.json ===
copy config.json "%DIST_DIR%\"

echo.
echo === Copying resources ===
xcopy /E /I /Y resources "%DIST_DIR%\resources"

echo.
echo === Copying packed environments ===
if exist "%RELEASE_ENVS%" (
    xcopy /E /I /Y "%RELEASE_ENVS%" "%DIST_DIR%\envs"
) else (
    echo WARNING: release_build\WAVES Processor\envs not found.
    echo Run 3_pack_envs.bat first.
)

echo.
echo === Creating logs folder ===
if not exist "%DIST_DIR%\logs" mkdir "%DIST_DIR%\logs"

echo.
echo Build complete.
echo Output: %DIST_DIR%
echo.
echo Test by running: "%DIST_DIR%\%APP_NAME%.exe"
echo Then run 5_build_installer.bat to create the setup wizard.
cd build_scripts
pause
