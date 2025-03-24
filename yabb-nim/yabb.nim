import std/[algorithm, osproc, strutils, strformat, os, math, sequtils, posix, times, logging, options, syncio]


try:
  putEnv("LC_ALL", "C")
except OSError:
  try:
    stderr.writeLine("Warning: Failed to set LC_ALL=C locale. Sorting and string operations may be inconsistent.")
  except IOError:
    discard # Can't do much if we can't write to stderr

type
  Config = object
    sourceVol: string
    snapDir: string
    destMount: string
    minFreeGb: int
    lockFile: string
    showProgressPercent: bool

  BackupState = object
    snapshotCreated: bool
    backupSuccessful: bool

  ShellCommand = object
    commands: seq[seq[string]]  # Sequence of commands (each command is a sequence of arguments)

const
  config = Config(
    sourceVol: "/data",
    snapDir: "/data/.snapshots",
    destMount: "/mnt/external",
    minFreeGb: 1,
    lockFile: "/var/lock/external-backup.lock",
    showProgressPercent: true
  )

var
  sourceBase: string
  snapName: string
  deltaSize: int
  backupState = BackupState()
  lockFileCreated = false

proc logError(msg: string) {.raises: [IOError, Exception].} =
  let timestamp = now().utc.format("yyyy-MM-dd'T'HH:mm:ss'Z'")
  stderr.writeLine(&"[{timestamp}] ERROR: {msg}")
  raise newException(Exception, msg)

proc formatBytes(bytes: int): string =
  const units = ["B", "KB", "MB", "GB", "TB"]
  var 
    value = bytes.float
    unitIdx = 0
  
  while value >= 1024 and unitIdx < units.high:
    value /= 1024
    unitIdx.inc
  
  let rounded = round(value * 10) / 10
  &"{rounded.formatFloat(ffDecimal, precision=1)} {units[unitIdx]}"

proc newShellCommand(): ShellCommand =
  result.commands = @[@[]]

proc addArg(cmd: var ShellCommand, arg: string) =
  if cmd.commands.len == 0:
    cmd.commands.add(@[])
  cmd.commands[^1].add(arg)

proc addCommand(cmd: var ShellCommand, command: string) =
  cmd.commands.add(@[command])

proc pipe(cmd: var ShellCommand) = 
  cmd.commands.add(@[])

proc quoteForShell(s: string): string =
  # More robust quoting implementation that handles all special shell characters
  if s.len == 0:
    return "''"
  if not s.contains({' ', '\t', '\n', '\r', ';', '&', '|', '<', '>', '(', ')', '$', '`', '\\', '"', '\'', '*', '?', '[', ']', '#', '~', '=', '%'}):
    return s
  return "'" & s.replace("'", "'\\''") & "'"

proc toString(cmd: ShellCommand): string =
  var resultCmd = ""
  for i, command in cmd.commands:
    if i > 0:
      resultCmd.add(" | ")
    resultCmd.add(command.map(quoteForShell).join(" "))
  return resultCmd

# Helper to simplify execution of shell commands
proc execShellCmd(cmd: ShellCommand, options: set[ProcessOption] = {poUsePath, poEvalCommand}): tuple[output: string, exitCode: int] =
  execCmdEx(cmd.toString(), options = options)

proc checkDestinationSpace(requiredBytes: int) =
  let buffer = 1_000_000_000  # 1GB buffer
  let requiredWithBuffer = requiredBytes + buffer
  
  var cmd = newShellCommand()
  cmd.addArg("btrfs")
  cmd.addArg("filesystem")
  cmd.addArg("usage")
  cmd.addArg("-b")
  cmd.addArg(config.destMount)
  
  let result = execShellCmd(cmd)
  
  if result.exitCode != 0:
    logError("Failed to check destination filesystem")
    # Program terminates here if error occurs
  
  let output = result.output
  var freeBytes: int = 0
  var foundFreeSpace = false
  
  for line in output.splitLines:
    if line.contains("Free (estimated):"):
      try:
        # The line format is: "Free (estimated):           12345678    (min: 12345678)"
        # Get all space-separated items
        let parts = line.splitWhitespace()
        
        # Find the actual bytes value (the number without parentheses)
        for i in 0..<parts.len:
          if parts[i] == "Free" and i+2 < parts.len:
            try:
              freeBytes = parseInt(parts[i+2])
              foundFreeSpace = true
              break
            except:
              continue
        
        if not foundFreeSpace and parts.len >= 3:
          # Fallback: Try the third item which is often the value
          try:
            freeBytes = parseInt(parts[2])
            foundFreeSpace = true
          except:
            discard
      except Exception as e:
        # Log the raw line to help with debugging
        logError(&"Could not parse free space from: '{line}'. Error: {e.msg}")
        # Program terminates here if parsing fails
      break
  
  if not foundFreeSpace:
    logError("Could not determine free space - 'Free (estimated):' not found in btrfs output")
    # Program terminates here if free space info not found
  
  # Display space information for both success and failure paths
  echo &"Space check: Required with buffer: {formatBytes(requiredWithBuffer)}, Available: {formatBytes(freeBytes)}"
  
  if freeBytes < requiredWithBuffer:
    logError(&"Insufficient space. Needed: {formatBytes(requiredWithBuffer)}, Available: {formatBytes(freeBytes)}")
    # Program terminates here if insufficient space
  
  # This will only execute if all checks above passed
  echo "Space check passed - sufficient free space available"

proc verifyUuids(sourcePath, destPath: string): bool =
  var sourceCmd = newShellCommand()
  sourceCmd.addArg("btrfs")
  sourceCmd.addArg("subvolume")
  sourceCmd.addArg("show")
  sourceCmd.addArg(sourcePath)
  
  var destCmd = newShellCommand()
  destCmd.addArg("btrfs")
  destCmd.addArg("subvolume")
  destCmd.addArg("show")
  destCmd.addArg(destPath)

  let 
    sourceResult = execShellCmd(sourceCmd)
    destResult = execShellCmd(destCmd)

  if sourceResult.exitCode != 0 or destResult.exitCode != 0:
    return false

  var sourceUuid, destUuid: string
  
  let sourceLines = sourceResult.output.splitLines.filterIt(it.contains("UUID:"))
  if sourceLines.len > 0:
    sourceUuid = sourceLines[0].splitWhitespace()[^1]
  
  let destReceivedLines = destResult.output.splitLines.filterIt(it.contains("Received UUID:"))
  if destReceivedLines.len > 0:
    destUuid = destReceivedLines[0].splitWhitespace()[^1]
  else:
    let destRegularLines = destResult.output.splitLines.filterIt(it.contains("UUID:"))
    if destRegularLines.len > 0:
      destUuid = destRegularLines[0].splitWhitespace()[^1]

  return sourceUuid == destUuid

proc checkDependencies() =
  let deps = ["btrfs", "pv"]
  for dep in deps:
    var cmd = newShellCommand()
    cmd.addArg("command")
    cmd.addArg("-v")
    cmd.addArg(dep)
    if execShellCmd(cmd).exitCode != 0:
      logError(&"{dep} required but not found")

proc checkMount(mountPath: string) =
  if not dirExists(mountPath):
    logError(&"Mount point {mountPath} does not exist")
  
  var mountpointCmd = newShellCommand()
  mountpointCmd.addArg("mountpoint")
  mountpointCmd.addArg("-q")
  mountpointCmd.addArg(mountPath)
  
  if execShellCmd(mountpointCmd).exitCode != 0:
    var mountCmd = newShellCommand()
    mountCmd.addArg("mount")
    mountCmd.addArg(mountPath)
    
    if execShellCmd(mountCmd).exitCode != 0:
      logError(&"Failed to mount {mountPath}")

proc acquireLock() =
  var originalUmask: Mode
  try:
    originalUmask = umask(0o177)  # Set restrictive permissions
    
    # Attempt to create the lock file with O_CREAT|O_EXCL for atomic creation
    let fd = posix.open(cstring(config.lockFile), O_CREAT or O_EXCL or O_WRONLY, 0o600)
    if fd == -1:
      let errCode = osLastError()
      if errCode == OSErrorCode(EEXIST):
        # File exists, check if process is still running
        try:
          var existingLockFile = open(config.lockFile, fmRead)
          defer: existingLockFile.close()
          
          let pidStr = existingLockFile.readLine()
          try:
            let pid = parseInt(pidStr)
            
            # Check if process is still running using kill with signal 0
            # (doesn't actually send a signal but checks if process exists)
            var killResult = posix.kill(pid.cint, 0)
            if killResult == 0:
              logError(&"Another backup is already in progress (PID: {pid})")
            else:
              # Process no longer exists, stale lock file
              removeFile(config.lockFile)
              # Try again with the stale lock file removed
              acquireLock()
              return
          except ValueError:
            logError("Lock file exists but contains invalid PID. If no backup is running, manually remove: " & config.lockFile)
        except IOError:
          logError("Lock file exists but cannot be read. If no backup is running, manually remove: " & config.lockFile)
        
        # If we get here, there's a problem with the lock file that we can't automatically resolve
        logError("Cannot acquire lock. If no backup is running, manually remove: " & config.lockFile)
      else:
        # Some other error occurred when trying to create the file
        logError(&"Failed to create lock file: {$strerror(errno)}")
    
    # Successfully created the file - write PID to it using Nim's file API
    defer: discard posix.close(fd)
    
    try:
      # Create a separate Nim file object for the already opened fd
      var pidFile = open(config.lockFile, fmWrite)
      defer: pidFile.close()
      
      # Write PID to lockfile
      pidFile.writeLine($getCurrentProcessId())
      pidFile.flushFile()
      
      # Use fcntl to place a lock on the file
      var fl: Tflock
      fl.l_type = cshort(F_WRLCK)
      fl.l_whence = cshort(SEEK_SET)
      fl.l_start = 0
      fl.l_len = 0
      
      let rc = fcntl(fd, F_SETLK, addr fl)
      if rc == -1:
        removeFile(config.lockFile)
        logError("Failed to acquire lock (fcntl error)")
      
      # If we reached here, we've successfully acquired the lock
      lockFileCreated = true
    except Exception as e:
      # Handle any exceptions during file operations
      removeFile(config.lockFile)
      logError(&"Failed to write PID to lock file: {e.msg}")
    
  finally:
    discard umask(originalUmask)

proc releaseLock() =
  if lockFileCreated:
    try:
      removeFile(config.lockFile)
      lockFileCreated = false
    except OSError:
      stderr.writeLine("Warning: Failed to remove lock file: " & config.lockFile)

proc findParentSnapshot(): Option[string] =
  # Use btrfs subvolume list with -t to get transaction IDs for reliable ordering
  var cmd = newShellCommand()
  cmd.addArg("btrfs")
  cmd.addArg("subvolume")
  cmd.addArg("list")
  cmd.addArg("-t")  # Include timestamps
  cmd.addArg("-o")  # Only list subvolumes in the specified path
  cmd.addArg(config.snapDir)
  
  let cmdResult = execShellCmd(cmd)
  if cmdResult.exitCode != 0:
    echo "Warning: Failed to get subvolume list. Falling back to filesystem metadata."
    # Fall back to old method if btrfs command fails
    var snapshots: seq[string]
    for (kind, path) in walkDir(config.snapDir, checkDir = true):
      if kind == pcDir and path.extractFilename.startsWith(sourceBase):
        snapshots.add(path)
    
    snapshots.sort(proc(a, b: string): int = cmp(getLastModificationTime(b), getLastModificationTime(a)))
    
    for snap in snapshots:
      if snap != config.snapDir / snapName:
        return some(snap.extractFilename)
    return none(string)
  
  # Process the output of btrfs subvolume list -t
  type SnapInfo = tuple[path: string, ctime: int64]
  var snapshotInfo: seq[SnapInfo] = @[]
  
  for line in cmdResult.output.splitLines():
    # Skip empty lines or header lines
    if line.len == 0 or line.startsWith("ID "):
      continue
      
    let parts = line.splitWhitespace()
    if parts.len >= 10:  # Make sure we have enough fields
      let 
        ctimeStr = parts[8]  # Creation time field
        snapPath = parts[^1]  # Path is the last field
        filename = snapPath.extractFilename()
      
      # Only consider snapshots with the right prefix and not the current one
      if filename.startsWith(sourceBase) and filename != snapName:
        try:
          let ctime = parseBiggestInt(ctimeStr)
          snapshotInfo.add((path: filename, ctime: ctime))
        except ValueError:
          # If we can't parse the timestamp, just skip this entry
          continue
  
  # Sort by creation timestamp (newest first)
  snapshotInfo.sort(proc(a, b: SnapInfo): int = cmp(b.ctime, a.ctime))
  
  # Return the most recent snapshot
  if snapshotInfo.len > 0:
    return some(snapshotInfo[0].path)
  
  return none(string)

proc estimateDeltaSize(parentPath, currentPath: string): int =
  var dryRunCmd = newShellCommand()
  dryRunCmd.addArg("btrfs")
  dryRunCmd.addArg("send")
  dryRunCmd.addArg("-p")
  dryRunCmd.addArg(parentPath)
  dryRunCmd.addArg("--no-data")
  dryRunCmd.addArg(currentPath)
  dryRunCmd.pipe()
  dryRunCmd.addArg("wc")
  dryRunCmd.addArg("-c")
  
  let dryRun = execShellCmd(dryRunCmd)
  
  var estimated = if dryRun.exitCode == 0:
    let bytes = dryRun.output.strip.parseInt
    bytes + bytes div 20
  else:
    var duCmd = newShellCommand()
    duCmd.addArg("du")
    duCmd.addArg("-sb")
    duCmd.addArg(currentPath)
    
    let duOutput = execShellCmd(duCmd).output.splitWhitespace()[0]
    duOutput.parseInt div 10

  # Enforce 10MB minimum
  estimated = max(estimated, 10_000_000)
  estimated

proc runPipelineWithStatusCheck(cmdPipeline: ShellCommand): tuple[output: string, statuses: seq[int]] =
  # Create a bash script that will execute our command and capture PIPESTATUS properly
  let bashScript = """
#!/usr/bin/env bash
set -o pipefail
# Run the command and capture its output
OUTPUT=$(""" & cmdPipeline.toString() & """ 2>&1 | grep -v 'write .* offset=')
# Store the exit status
RESULT=$?
# Output a delimiter we can parse for
echo "---COMMAND_OUTPUT_END---"
# Echo the original output
echo "$OUTPUT"
# Output another delimiter for PIPESTATUS
echo "---PIPESTATUS_START---"
# Output each value in PIPESTATUS
for status in "${PIPESTATUS[@]}"; do
  echo "$status"
done
# Return the overall result
exit $RESULT
"""

  # Create a secure temporary file using mkstemp
  var tmpFilename = getTempDir() / "yabb_XXXXXX"
  var fd = posix.mkstemp(cstring(tmpFilename))
  if fd == -1:
    raise newException(IOError, "Failed to create secure temporary file: " & $strerror(errno))
  
  # Write script content to the temporary file
  try:
    # Use Nim's higher-level file API instead of direct posix.write
    discard posix.close(fd)
    fd = -1  # Mark as closed
    
    # Write the script to the file
    writeFile(tmpFilename, bashScript)
    
    # Set executable permission
    discard chmod(cstring(tmpFilename), 0o700)  # rwx for user only
    
    # Run the script
    var scriptCmd = newShellCommand()
    scriptCmd.addArg(tmpFilename)
    let scriptResult = execShellCmd(scriptCmd)
    
    # Parse the output
    var 
      output = ""
      statuses: seq[int] = @[]
      inPipestatus = false
      
    for line in scriptResult.output.splitLines():
      if line == "---COMMAND_OUTPUT_END---":
        inPipestatus = false
        continue
      elif line == "---PIPESTATUS_START---":
        inPipestatus = true
        continue
      
      if inPipestatus:
        try:
          statuses.add(parseInt(line.strip()))
        except ValueError:
          discard
      else:
        if output.len > 0:
          output.add("\n")
        output.add(line)
    
    # If we somehow didn't parse any statuses, at least return the overall result
    if statuses.len == 0:
      statuses.add(scriptResult.exitCode)
      
    return (output: output, statuses: statuses)
  
  finally:
    # Always clean up
    if fd != -1:
      discard posix.close(fd)  # Close fd if it's still open
    
    try:
      removeFile(tmpFilename)
    except:
      echo "Warning: Could not remove temporary file: " & tmpFilename

proc performBackup(parentSnapshot: Option[string]) =
  let pvArgs = if config.showProgressPercent: "-petab" else: "-b"
  var cmdPipeline = newShellCommand()
  var backupType = "full"
  
  if parentSnapshot.isSome:
    backupType = "incremental"
    let parent = parentSnapshot.get
    
    let destParentPath = config.destMount / parent
    if not dirExists(destParentPath):
      logError(&"Parent snapshot {parent} missing from destination")
    
    let sourceParentPath = config.snapDir / parent
    if not verifyUuids(sourceParentPath, destParentPath):
      logError("Parent snapshot UUID mismatch")
    
    deltaSize = estimateDeltaSize(sourceParentPath, config.snapDir / snapName)
    checkDestinationSpace(deltaSize)
    
    # Build the send command
    cmdPipeline.addArg("btrfs")
    cmdPipeline.addArg("send")
    cmdPipeline.addArg("-p")
    cmdPipeline.addArg(sourceParentPath)
    cmdPipeline.addArg(config.snapDir / snapName)
    
    # Add the pipe and pv command
    cmdPipeline.pipe()
    cmdPipeline.addArg("pv")
    for arg in pvArgs.split():
      cmdPipeline.addArg(arg)
    
    # Add the receive command
    cmdPipeline.pipe()
    cmdPipeline.addArg("btrfs")
    cmdPipeline.addArg("receive")
    cmdPipeline.addArg(config.destMount)
    
  else:
    # Calculate size for full backup
    var sizeCmd = newShellCommand()
    sizeCmd.addArg("du")
    sizeCmd.addArg("-sb")
    sizeCmd.addArg(config.snapDir / snapName)
    
    let sizeResult = execShellCmd(sizeCmd)
    if sizeResult.exitCode != 0:
      logError("Failed to calculate backup size")
    
    deltaSize = sizeResult.output.splitWhitespace[0].parseInt
    checkDestinationSpace(deltaSize)
    
    # Build full backup command
    cmdPipeline.addArg("btrfs")
    cmdPipeline.addArg("send")
    cmdPipeline.addArg(config.snapDir / snapName)
    
    # Add the pipe and pv command
    cmdPipeline.pipe()
    cmdPipeline.addArg("pv")
    for arg in pvArgs.split():
      cmdPipeline.addArg(arg)
    
    # Add the receive command
    cmdPipeline.pipe()
    cmdPipeline.addArg("btrfs")
    cmdPipeline.addArg("receive")
    cmdPipeline.addArg(config.destMount)

  echo &"Starting {backupType} send with progress monitoring"
  
  # Run with status checking
  let result = runPipelineWithStatusCheck(cmdPipeline)
  var pipelineSuccess = true
  var errorMsg = ""
  
  # Check each pipeline component's status
  if result.statuses.len >= 3:
    # Check send status (first component)
    if result.statuses[0] != 0:
      pipelineSuccess = false
      errorMsg = &"btrfs send failed with code {result.statuses[0]}"
    # Check pv status (second component)
    elif result.statuses[1] != 0:
      pipelineSuccess = false
      errorMsg = &"Progress monitor (pv) failed with code {result.statuses[1]}"
    # Check receive status (third component)
    elif result.statuses[2] != 0:
      pipelineSuccess = false
      errorMsg = &"btrfs receive failed with code {result.statuses[2]}"
  elif result.statuses.len > 0:
    # If we have at least one status code
    if result.statuses[0] != 0:
      pipelineSuccess = false
      errorMsg = &"Backup pipeline failed with code {result.statuses[0]}"
  else:
    # If we got no status information at all (this should never happen with our implementation)
    pipelineSuccess = false
    errorMsg = "Backup pipeline failed: could not determine exit status"
  
  # Handle failure or success properly
  if not pipelineSuccess:
    logError(errorMsg)
  
  # If we get here, the pipeline was successful!
  echo &"✓ Backup data transfer completed successfully ({backupType}, {formatBytes(deltaSize)})"
  echo "Verifying destination snapshot integrity..."

proc cleanup() =
  if backupState.snapshotCreated and not backupState.backupSuccessful:
    proc deleteWithRetries(thePath: string) {.raises: [OSError, IOError].} =
      var retries = 3
      while retries > 0:
        var delCmd = newShellCommand()
        delCmd.addArg("btrfs")
        delCmd.addArg("subvolume")
        delCmd.addArg("delete")
        delCmd.addArg(thePath)
        
        if execShellCmd(delCmd).exitCode == 0:
          break
        dec retries
        sleep(1000)
    
    deleteWithRetries(config.snapDir / snapName)
    deleteWithRetries(config.destMount / snapName)

proc quoteIfNeeded(s: string): string =
  # Mimic Bash's ${VAR@Q} quoting for special characters
  if s.contains({' ', '\t', '\n', '*', '?', '[', ']', '<', '>', '|', ';', '&', '$', '(', ')', '`', '"', '\''}) or s == "":
    return "'" & s.replace("'", "'\\''") & "'"
  return s

when isMainModule:
  var exitCode = 0
  
  try:
    # Add proper configuration for the file logger
    addHandler(newFileLogger(
      filename = "yabb.log",
      mode = fmAppend,
      levelThreshold = lvlInfo,
      fmtStr = "$datetime - $levelname: $msg",
      bufSize = 0
    ))
    
    try:
      setCurrentDir(getAppDir())
    except OSError:
      stderr.writeLine("Warning: Could not change to application directory")
    
    sourceBase = config.sourceVol.extractFilename
    snapName = &"{sourceBase}.{now().utc.format(\"yyyy-MM-dd'T'HH:mm:ss'Z'\")}"
  
    checkDependencies()
    checkMount(config.sourceVol)
    checkMount(config.destMount)
    acquireLock()
    
    var snapCmd = newShellCommand()
    snapCmd.addArg("btrfs")
    snapCmd.addArg("subvolume")
    snapCmd.addArg("snapshot")
    snapCmd.addArg("-r")
    snapCmd.addArg(config.sourceVol)
    snapCmd.addArg(config.snapDir / snapName)
    
    let snapResult = execShellCmd(snapCmd)
    if snapResult.exitCode != 0:
      logError("Failed to create snapshot")
    backupState.snapshotCreated = true
    
    let parentSnapshot = findParentSnapshot()
    performBackup(parentSnapshot)
    
    let sourceSnapPath = config.snapDir / snapName
    let destSnapPath = config.destMount / snapName
    
    if not verifyUuids(sourceSnapPath, destSnapPath):
      backupState.backupSuccessful = false  # Mark as failed
      logError("Destination snapshot UUID mismatch")
    
    # This is now the only place where we set backupState.backupSuccessful = true
    # It will only be reached if both the backup pipeline and UUID verification succeeded
    backupState.backupSuccessful = true
    let backupType = if parentSnapshot.isSome: "incremental" else: "full"
    echo &"Backup successful: {quoteIfNeeded(snapName)} ({backupType})!"
    
  except IOError as e:
    exitCode = 1
    try:
      stderr.writeLine("ERROR: Failed to set up logging. Check permissions for creating yabb.log: " & e.msg)
    except IOError:
      # Can't do much if stderr is unavailable
      discard
    
    try:
      cleanup()
    except [OSError, IOError]:
      try:
        stderr.writeLine("ERROR: Additional failure during cleanup")
      except IOError:
        discard
  
  except Exception as e:
    # Handle any other exceptions
    exitCode = 1
    let msg = e.msg
    try:
      stderr.writeLine(&"ERROR: {msg}")
    except IOError:
      discard
    
    try:
      cleanup()
    except [OSError, IOError]:
      try:
        stderr.writeLine("ERROR: Additional failure during cleanup")
      except IOError:
        discard
  
  finally:
    # This will always execute, regardless of success or failure
    releaseLock()
    # Use the exit code we set based on exceptions
    quit(exitCode)
