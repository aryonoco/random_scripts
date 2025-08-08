## Utility functions for YABB
import std/[strutils, strformat, math]

proc formatBytes*(bytes: int): string =
  const units: array[0..4, string] = ["B", "KB", "MB", "GB", "TB"]
  var 
    value: float = bytes.float
    unitIdx: int = 0
  
  while value >= 1024 and unitIdx < units.high:
    value /= 1024
    unitIdx.inc
  
  let rounded: float = round(value * 10) / 10
  &"{rounded.formatFloat(ffDecimal, precision=1)} {units[unitIdx]}"

proc quoteForShell*(s: string): string =
  # More robust quoting implementation that handles all special shell characters
  if s.len == 0:
    return "''"
  if not s.contains({' ', '\t', '\n', '\r', ';', '&', '|', '<', '>', '(', ')', '$', '`', '\\', '"', '\'', '*', '?', '[', ']', '#', '~', '=', '%'}):
    return s
  return "'" & s.replace("'", "'\\''") & "'" 