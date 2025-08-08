## Snapshot management for YABB
import ./types
import ./shell
import ./logging
import ./utils
import std/[os, strformat, times, options, algorithm]

proc findParentSnapshot*(snapDir, destMount, sourceBase, snapName: string): Option[string] =
  echo "Looking for parent snapshot..."
  
  # First get a list of all snapshots from filesystem
  var availableSnapshots: seq[string] = @[]
  for (kind, path) in walkDir(snapDir, checkDir = true):
    if kind == pcDir and path.extractFilename.startsWith(sourceBase):
      let snapBasename = path.extractFilename
      # Make sure it's not the current snapshot we're creating
      if snapBasename != snapName:
        # Also verify it exists on destination for incremental backup
        if dirExists(destMount / snapBasename):
          availableSnapshots.add(snapBasename)
          echo &"  Found snapshot: {snapBasename}"
        else:
          echo &"  Skipping snapshot not on destination: {snapBasename}"
  
  if availableSnapshots.len == 0:
    echo "No existing snapshots found on both source and destination"
    return none(string)
  
  # Sort by timestamp in the filename (most recent first)
  availableSnapshots.sort(proc(a, b: string): int =
    # Extract timestamp part after the prefix
    let 
      tsA = a[sourceBase.len+1 .. ^1]
      tsB = b[sourceBase.len+1 .. ^1]
    # Reverse comparison (newest first)
    result = cmp(tsB, tsA)
  )
  
  echo &"Selected parent snapshot: {availableSnapshots[0]}"
  return some(availableSnapshots[0])

proc createSnapshot*(sourceVol, snapDir: string): string =
  let sourceBase = sourceVol.extractFilename
  let snapName = &"{sourceBase}.{now().utc.format(\"yyyy-MM-dd'T'HH:mm:ss'Z'\")}"
  
  var snapCmd: ShellCommand = newShellCommand()
  snapCmd.addArg("btrfs")
  snapCmd.addArg("subvolume")
  snapCmd.addArg("snapshot")
  snapCmd.addArg("-r")
  snapCmd.addArg(sourceVol)
  snapCmd.addArg(snapDir / snapName)
  
  let snapResult: tuple[output: string, exitCode: int] = execShellCmd(snapCmd)
  if snapResult.exitCode != 0:
    logError("Failed to create snapshot")
  
  return snapName 