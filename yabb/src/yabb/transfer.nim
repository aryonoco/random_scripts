## BTRFS send/receive operations for YABB
import ./types
import ./shell
import ./logging
import ./utils
import std/[os, strformat, options]

proc verifyUuids*(sourcePath, destPath: string): bool =
  var sourceCmd: ShellCommand = newShellCommand()
  sourceCmd.addArg("btrfs")
  sourceCmd.addArg("subvolume")
  sourceCmd.addArg("show")
  sourceCmd.addArg(sourcePath)
  
  var destCmd: ShellCommand = newShellCommand()
  destCmd.addArg("btrfs")
  destCmd.addArg("subvolume")
  destCmd.addArg("show")
  destCmd.addArg(destPath)

  let 
    sourceResult: tuple[output: string, exitCode: int] = execShellCmd(sourceCmd)
    destResult: tuple[output: string, exitCode: int] = execShellCmd(destCmd)

  if sourceResult.exitCode != 0 or destResult.exitCode != 0:
    return false

  var sourceUuid, destUuid: string
  
  let sourceLines: seq[string] = sourceResult.output.splitLines.filterIt(it.contains("UUID:"))
  if sourceLines.len > 0:
    sourceUuid = sourceLines[0].splitWhitespace()[^1]
  
  let destReceivedLines: seq[string] = destResult.output.splitLines.filterIt(it.contains("Received UUID:"))
  if destReceivedLines.len > 0:
    destUuid = destReceivedLines[0].splitWhitespace()[^1]
  else:
    let destRegularLines: seq[string] = destResult.output.splitLines.filterIt(it.contains("UUID:"))
    if destRegularLines.len > 0:
      destUuid = destRegularLines[0].splitWhitespace()[^1]

  return sourceUuid == destUuid

proc checkDestinationSpace*(destMount: string, requiredBytes: int) =
  let buffer: int = 1_000_000_000  # 1GB buffer
  let requiredWithBuffer: int = requiredBytes + buffer
  
  var cmd: ShellCommand = newShellCommand()
  cmd.addArg("btrfs")
  cmd.addArg("filesystem")
  cmd.addArg("usage")
  cmd.addArg("-b")
  cmd.addArg(destMount)
  
  let result: tuple[output: string, exitCode: int] = execShellCmd(cmd)
  
  if result.exitCode != 0:
    logError("Failed to check destination filesystem")
    # Program terminates here if error occurs
  
  let output: string = result.output
  var freeBytes: int = 0
  var foundFreeSpace: bool = false
  
  for line in output.splitLines:
    if line.contains("Free (estimated):"):
      try:
        # The line format is: "Free (estimated):           12345678    (min: 12345678)"
        # Get all space-separated items
        let parts: seq[string] = line.splitWhitespace()
        
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

proc estimateDeltaSize*(parentPath, currentPath: string): int =
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
  
  let dryRun: tuple[output: string, exitCode: int] = execShellCmd(dryRunCmd)
  
  var estimated: int = if dryRun.exitCode == 0:
    let bytes: int = dryRun.output.strip.parseInt
    bytes + bytes div 20
  else:
    var duCmd: ShellCommand = newShellCommand()
    duCmd.addArg("du")
    duCmd.addArg("-sb")
    duCmd.addArg(currentPath)
    
    let duOutput: string = execShellCmd(duCmd).output.splitWhitespace()[0]
    duOutput.parseInt div 10

  # Enforce 10MB minimum
  result = max(estimated, 10_000_000)

proc doFullBackup*(cfg: Config, snapName: string): bool =
  # Calculate size for full backup
  var sizeCmd: ShellCommand = newShellCommand()
  sizeCmd.addArg("du")
  sizeCmd.addArg("-sb")
  sizeCmd.addArg(cfg.snapDir / snapName)
  
  let sizeResult: tuple[output: string, exitCode: int] = execShellCmd(sizeCmd)
  if sizeResult.exitCode != 0:
    logError("Failed to calculate backup size")
  
  let deltaSize = sizeResult.output.splitWhitespace[0].parseInt
  checkDestinationSpace(cfg.destMount, deltaSize)
  
  # Build full backup command
  let pvArgs: string = if cfg.showProgressPercent: "-petab" else: "-b"
  var cmdPipeline: ShellCommand = newShellCommand()
  
  # Build the send command
  cmdPipeline.addArg("btrfs")
  cmdPipeline.addArg("send")
  cmdPipeline.addArg(cfg.snapDir / snapName)
  
  # Add the pipe and pv command
  cmdPipeline.pipe()
  cmdPipeline.addArg("pv")
  for arg in pvArgs.split():
    cmdPipeline.addArg(arg)
  
  # Add the receive command
  cmdPipeline.pipe()
  cmdPipeline.addArg("btrfs")
  cmdPipeline.addArg("receive")
  cmdPipeline.addArg(cfg.destMount)

  echo "Starting full send with progress monitoring"
  
  # Run with status checking
  let result: tuple[output: string, statuses: seq[int]] = runPipelineWithStatusCheck(cmdPipeline)
  var pipelineSuccess: bool = true
  var errorMsg: string = ""
  
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
  echo &"✓ Backup data transfer completed successfully (full, {formatBytes(deltaSize)})"
  echo "Verifying destination snapshot integrity..."
  
  return pipelineSuccess

proc doIncrementalBackup*(cfg: Config, parent, snapName: string): bool =
  let parentPath = cfg.snapDir / parent
  let snapPath = cfg.snapDir / snapName
  
  # Check destination parent snapshot
  let destParentPath: string = cfg.destMount / parent
  if not dirExists(destParentPath):
    logError(&"Parent snapshot {parent} missing from destination")
  
  if not verifyUuids(parentPath, destParentPath):
    logError("Parent snapshot UUID mismatch")
  
  # Calculate delta size
  let deltaSize = estimateDeltaSize(parentPath, snapPath)
  checkDestinationSpace(cfg.destMount, deltaSize)
  
  # Build the command pipeline
  let pvArgs: string = if cfg.showProgressPercent: "-petab" else: "-b"
  var cmdPipeline: ShellCommand = newShellCommand()
  
  # Build the send command
  cmdPipeline.addArg("btrfs")
  cmdPipeline.addArg("send")
  cmdPipeline.addArg("-p")
  cmdPipeline.addArg(parentPath)
  cmdPipeline.addArg(snapPath)
  
  # Add the pipe and pv command
  cmdPipeline.pipe()
  cmdPipeline.addArg("pv")
  for arg in pvArgs.split():
    cmdPipeline.addArg(arg)
  
  # Add the receive command
  cmdPipeline.pipe()
  cmdPipeline.addArg("btrfs")
  cmdPipeline.addArg("receive")
  cmdPipeline.addArg(cfg.destMount)

  echo "Starting incremental send with progress monitoring"
  
  # Run with status checking
  let result: tuple[output: string, statuses: seq[int]] = runPipelineWithStatusCheck(cmdPipeline)
  var pipelineSuccess: bool = true
  var errorMsg: string = ""
  
  # Check each pipeline component's status (similar to doFullBackup)
  if result.statuses.len >= 3:
    if result.statuses[0] != 0:
      pipelineSuccess = false
      errorMsg = &"btrfs send failed with code {result.statuses[0]}"
    elif result.statuses[1] != 0:
      pipelineSuccess = false
      errorMsg = &"Progress monitor (pv) failed with code {result.statuses[1]}"
    elif result.statuses[2] != 0:
      pipelineSuccess = false
      errorMsg = &"btrfs receive failed with code {result.statuses[2]}"
  elif result.statuses.len > 0:
    if result.statuses[0] != 0:
      pipelineSuccess = false
      errorMsg = &"Backup pipeline failed with code {result.statuses[0]}"
  else:
    pipelineSuccess = false
    errorMsg = "Backup pipeline failed: could not determine exit status"
  
  if not pipelineSuccess:
    logError(errorMsg)
  
  echo &"✓ Backup data transfer completed successfully (incremental, {formatBytes(deltaSize)})"
  echo "Verifying destination snapshot integrity..."
  
  return pipelineSuccess 