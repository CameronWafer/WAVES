; ============================================================
; WAVES Processor - Inno Setup installer script
; Builds: WAVES_Processor_Setup_v1.2.exe
;
; Installs to %LOCALAPPDATA%\WAVES Processor (no admin required).
; No conda-unpack needed: app invokes python.exe -m <module> directly.
; Creates desktop and Start Menu shortcuts.
; ============================================================

#define AppName      "WAVES Processor"
#define AppVersion   "1.3"
#define AppPublisher "WAVES Research"
#define AppExeName   "WAVES Processor.exe"
#define DistDir      "D:\WAVES Processor"

[Setup]
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={localappdata}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
OutputDir=D:\WAVES Installer Output
OutputBaseFilename=WAVES_Processor_Setup_v{#AppVersion}
Compression=lzma2/ultra64
SolidCompression=yes
; Install to user's LocalAppData — no admin rights required
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
; Show a friendly wizard
WizardStyle=modern
; Icon (set after you have one)
; SetupIconFile=..\resources\app_icon.ico

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Additional shortcuts:"

[Files]
; Main executable and PyInstaller _internal folder
Source: "{#DistDir}\{#AppExeName}";         DestDir: "{app}";          Flags: ignoreversion
Source: "{#DistDir}\_internal\*";           DestDir: "{app}\_internal"; Flags: ignoreversion recursesubdirs createallsubdirs

; Config
Source: "{#DistDir}\config.json";           DestDir: "{app}";          Flags: ignoreversion

; Bundled Conda environments (large — may take a few minutes to copy)
Source: "{#DistDir}\envs\*";                DestDir: "{app}\envs";     Flags: ignoreversion recursesubdirs createallsubdirs

; Resources (uncomment when resources folder is populated)
; Source: "{#DistDir}\resources\*"; DestDir: "{app}\resources"; Flags: ignoreversion recursesubdirs createallsubdirs

[Dirs]
; Create an empty logs folder so the app can write logs immediately
Name: "{app}\logs"

[Icons]
Name: "{autoprograms}\{#AppName}"; Filename: "{app}\{#AppExeName}"
Name: "{autodesktop}\{#AppName}";  Filename: "{app}\{#AppExeName}"; Tasks: desktopicon

[Run]
; NOTE: conda-unpack is no longer needed. The app calls python.exe -m actinet.actinet
; and python.exe -m accelerometer.accProcess directly. python.exe is a native binary
; with no hardcoded paths, so it works on any install location without path fixup.

; Offer to launch the app after install
Filename: "{app}\{#AppExeName}"; Description: "Launch {#AppName}"; Flags: nowait postinstall skipifsilent
