; ============================================================
; WAVES Processor - Inno Setup installer script
; Builds: WAVES_Processor_Setup_v1.0.0.exe
;
; Installs to %LOCALAPPDATA%\WAVES Processor (no admin required).
; Runs conda-unpack on both environments after copying files.
; Creates desktop and Start Menu shortcuts.
; ============================================================

#define AppName      "WAVES Processor"
#define AppVersion   "1.0.0"
#define AppPublisher "WAVES Research"
#define AppExeName   "WAVES Processor.exe"
#define DistDir      "..\dist\WAVES Processor"

[Setup]
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={localappdata}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
OutputDir=Output
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

; Resources
Source: "{#DistDir}\resources\*";           DestDir: "{app}\resources"; Flags: ignoreversion recursesubdirs createallsubdirs; Excludes: "*.ico"
; Source: "..\resources\app_icon.ico";      DestDir: "{app}\resources"; Flags: ignoreversion

[Dirs]
; Create an empty logs folder so the app can write logs immediately
Name: "{app}\logs"

[Icons]
Name: "{autoprograms}\{#AppName}"; Filename: "{app}\{#AppExeName}"
Name: "{autodesktop}\{#AppName}";  Filename: "{app}\{#AppExeName}"; Tasks: desktopicon

[Run]
; Fix hardcoded paths inside the packed Conda environments.
; This MUST run after files are copied and BEFORE the app first launches.
Filename: "{app}\envs\WAVES_actinet\Scripts\conda-unpack.exe";
  Parameters: ""; \
  StatusMsg: "Configuring ActiNet environment (this may take a moment)..."; \
  Flags: runhidden waituntilterminated; \
  Check: FileExists(ExpandConstant('{app}\envs\WAVES_actinet\Scripts\conda-unpack.exe'))

Filename: "{app}\envs\WAVES_accelerometer\Scripts\conda-unpack.exe";
  Parameters: ""; \
  StatusMsg: "Configuring Accelerometer environment (this may take a moment)..."; \
  Flags: runhidden waituntilterminated; \
  Check: FileExists(ExpandConstant('{app}\envs\WAVES_accelerometer\Scripts\conda-unpack.exe'))

; Offer to launch the app after install
Filename: "{app}\{#AppExeName}"; \
  Description: "Launch {#AppName}"; \
  Flags: nowait postinstall skipifsilent
