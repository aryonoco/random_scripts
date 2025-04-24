## Type definitions for YABB

type
  Config* = object
    sourceVol*: string
    snapDir*: string
    destMount*: string
    minFreeGb*: int
    lockFile*: string
    showProgressPercent*: bool

  BackupState* = object
    snapshotCreated*: bool
    backupSuccessful*: bool

  ShellCommand* = object
    commands*: seq[seq[string]]  # Sequence of commands (each command is a sequence of arguments) 