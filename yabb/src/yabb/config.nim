## Configuration management for YABB
import ./types
import std/[os]

const
  defaultConfig*: Config = Config(
    sourceVol: "/data",
    snapDir: "/data/.snapshots",
    destMount: "/mnt/external",
    minFreeGb: 1,
    lockFile: "/var/lock/external-backup.lock",
    showProgressPercent: true
  )

proc loadConfig*(): Config {.raises: [].} =
  # For now just return the default config
  # In the future, this could load from file or environment variables
  result = defaultConfig
  
  # Set locale for consistent sorting and string operations
  try:
    putEnv("LC_ALL", "C")
  except OSError:
    try:
      stderr.writeLine("Warning: Failed to set LC_ALL=C locale. Sorting and string operations may be inconsistent.")
    except IOError:
      discard # Can't do much if we can't write to stderr 