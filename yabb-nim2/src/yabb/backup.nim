## Core backup functionality for YABB
import ./types
import ./config
import ./shell
import ./snapshot
import ./transfer
import ./locking
import ./logging
import ./utils
import std/[os, osproc, options, strformat]

var backupState: BackupState

proc checkDependencies*() =
  let deps: array[0..1, string] = ["btrfs", "pv"]
  for dep in deps:
    var cmd: ShellCommand = newShellCommand()
    cmd.addArg("command")
    cmd.addArg("-v")
    cmd.addArg(dep)
    if execShellCmd(cmd).exitCode != 0:
      logError(&"{dep} required but not found")

proc checkMount*(mountPath: string) =
  if not dirExists(mountPath):
    logError(&"Mount point {mountPath} does not exist")
  
  var mountpointCmd: ShellCommand = newShellCommand()
  mountpointCmd.addArg("mountpoint")
  mountpointCmd.addArg("-q")
  mountpointCmd.addArg(mountPath)
  
  if execShellCmd(mountpointCmd).exitCode != 0:
    var mountCmd: ShellCommand = newShellCommand()
    mountCmd.addArg("mount")
    mountCmd.addArg(mountPath)
    
    if execShellCmd(mountCmd).exitCode != 0:
      logError(&"Failed to mount {mountPath}")

proc cleanup*(cfg: Config, snapName: string) =
  if backupState.snapshotCreated and not backupState.backupSuccessful:
    proc deleteWithRetries(thePath: string) {.raises: [OSError, IOError].} =
      var retries: int = 3
      while retries > 0:
        var delCmd: ShellCommand = newShellCommand()
        delCmd.addArg("btrfs")
        delCmd.addArg("subvolume")
        delCmd.addArg("delete")
        delCmd.addArg(thePath)
        
        if execShellCmd(delCmd).exitCode == 0:
          break
        dec retries
        sleep(1000)
    
    deleteWithRetries(cfg.snapDir / snapName)
    deleteWithRetries(cfg.destMount / snapName)

proc performBackup*(cfg: Config) =
  let sourceBase = cfg.sourceVol.extractFilename
  let snapName = createSnapshot(cfg.sourceVol, cfg.snapDir)
  backupState.snapshotCreated = true
  
  # Look for parent snapshot for incremental backup
  let parentSnapshot = findParentSnapshot(cfg.snapDir, cfg.destMount, sourceBase, snapName)
  
  # Perform backup (incremental or full)
  var backupSuccess = false
  if parentSnapshot.isSome:
    backupSuccess = doIncrementalBackup(cfg, parentSnapshot.get, snapName)
  else:
    backupSuccess = doFullBackup(cfg, snapName)
  
  # Verify success
  if backupSuccess:
    let sourceSnapPath = cfg.snapDir / snapName
    let destSnapPath = cfg.destMount / snapName
    
    if not verifyUuids(sourceSnapPath, destSnapPath):
      logError("Destination snapshot UUID mismatch")
    
    # Mark backup as successful (only if we reach this point)
    backupState.backupSuccessful = true
    let backupType = if parentSnapshot.isSome: "incremental" else: "full"
    echo &"Backup successful: '{snapName}' ({backupType})!"
  else:
    cleanup(cfg, snapName)
    logError("Backup failed") 