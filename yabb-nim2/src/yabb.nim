## Yet Another BTRFS Backup tool
import yabb/[types, config, backup, logging, locking]
import std/[os]

proc main() =
  var exitCode: int = 0
  let cfg = loadConfig()
  
  try:
    setupLogging()
    
    try:
      setCurrentDir(getAppDir())
    except OSError:
      stderr.writeLine("Warning: Could not change to application directory")
    
    checkDependencies()
    checkMount(cfg.sourceVol)
    checkMount(cfg.destMount)
    acquireLock(cfg.lockFile)
    
    performBackup(cfg)
    
  except IOError as e:
    exitCode = 1
    try:
      stderr.writeLine("ERROR: Failed to set up logging. Check permissions for creating yabb.log: " & e.msg)
    except IOError:
      # Can't do much if stderr is unavailable
      discard
  
  except Exception as e:
    # Handle any other exceptions
    exitCode = 1
    let msg: string = e.msg
    try:
      stderr.writeLine(&"ERROR: {msg}")
    except IOError:
      discard
  
  finally:
    # This will always execute, regardless of success or failure
    releaseLock(cfg.lockFile)
    # Use the exit code we set based on exceptions
    quit(exitCode)

when isMainModule:
  main() 