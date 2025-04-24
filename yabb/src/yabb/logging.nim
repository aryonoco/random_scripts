## Logging utilities for YABB
import std/[logging, times, strformat, syncio]

proc setupLogging*() {.raises: [IOError].} =
  addHandler(newFileLogger(
    filename = "yabb.log",
    mode = fmAppend,
    levelThreshold = lvlInfo,
    fmtStr = "$datetime - $levelname: $msg",
    bufSize = 0
  ))

proc logError*(msg: string) {.raises: [IOError, Exception].} =
  let timestamp: string = now().utc.format("yyyy-MM-dd'T'HH:mm:ss'Z'")
  stderr.writeLine(&"[{timestamp}] ERROR: {msg}")
  raise newException(Exception, msg) 