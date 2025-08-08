## Lock file management for YABB
import ./logging
import std/[os, posix, strformat, strutils, syncio]

var lockFileCreated: bool = false

proc acquireLock*(lockFile: string) =
  var originalUmask: Mode
  try:
    originalUmask = umask(0o177)  # Set restrictive permissions
    
    # Attempt to create the lock file with O_CREAT|O_EXCL for atomic creation
    let fd: cint = posix.open(cstring(lockFile), O_CREAT or O_EXCL or O_WRONLY, 0o600)
    if fd == -1:
      let errCode: OSErrorCode = osLastError()
      if errCode == OSErrorCode(EEXIST):
        # File exists, check if process is still running
        try:
          var existingLockFile: File = open(lockFile, fmRead)
          defer: existingLockFile.close()
          
          let pidStr: string = existingLockFile.readLine()
          try:
            let pid: int = parseInt(pidStr)
            
            # Check if process is still running using kill with signal 0
            # (doesn't actually send a signal but checks if process exists)
            var killResult = posix.kill(pid.cint, 0)
            if killResult == 0:
              logError(&"Another backup is already in progress (PID: {pid})")
            else:
              # Process no longer exists, stale lock file
              removeFile(lockFile)
              # Try again with the stale lock file removed
              acquireLock(lockFile)
              return
          except ValueError:
            logError("Lock file exists but contains invalid PID. If no backup is running, manually remove: " & lockFile)
        except IOError:
          logError("Lock file exists but cannot be read. If no backup is running, manually remove: " & lockFile)
        
        # If we get here, there's a problem with the lock file that we can't automatically resolve
        logError("Cannot acquire lock. If no backup is running, manually remove: " & lockFile)
      else:
        # Some other error occurred when trying to create the file
        logError(&"Failed to create lock file: {$strerror(errno)}")
    
    # Successfully created the file - write PID to it using Nim's file API
    defer: discard posix.close(fd)
    
    try:
      # Create a separate Nim file object for the already opened fd
      var pidFile: File = open(lockFile, fmWrite)
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
      
      let rc: cint = fcntl(fd, F_SETLK, addr fl)
      if rc == -1:
        removeFile(lockFile)
        logError("Failed to acquire lock (fcntl error)")
      
      # If we reached here, we've successfully acquired the lock
      lockFileCreated = true
    except Exception as e:
      # Handle any exceptions during file operations
      removeFile(lockFile)
      logError(&"Failed to write PID to lock file: {e.msg}")
    
  finally:
    discard umask(originalUmask)

proc releaseLock*(lockFile: string) =
  if lockFileCreated:
    try:
      removeFile(lockFile)
      lockFileCreated = false
    except OSError:
      stderr.writeLine("Warning: Failed to remove lock file: " & lockFile) 