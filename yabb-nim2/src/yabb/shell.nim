## Shell command utilities for YABB
import ./types
import ./utils
import std/[osproc, strutils, posix, os, syncio]

proc newShellCommand*(): ShellCommand =
  result.commands = @[@[]]

proc addArg*(cmd: var ShellCommand, arg: string) =
  if cmd.commands.len == 0:
    cmd.commands.add(@[])
  cmd.commands[^1].add(arg)

proc addCommand*(cmd: var ShellCommand, command: string) =
  cmd.commands.add(@[command])

proc pipe*(cmd: var ShellCommand) = 
  cmd.commands.add(@[])

proc toString*(cmd: ShellCommand): string =
  var resultCmd: string = ""
  for i, command in cmd.commands:
    if i > 0:
      resultCmd.add(" | ")
    resultCmd.add(command.map(quoteForShell).join(" "))
  return resultCmd

# Helper to simplify execution of shell commands
proc execShellCmd*(cmd: ShellCommand, options: set[ProcessOption] = {poUsePath, poEvalCommand}): tuple[output: string, exitCode: int] =
  execCmdEx(cmd.toString(), options = options)

proc runPipelineWithStatusCheck*(cmdPipeline: ShellCommand): tuple[output: string, statuses: seq[int]] =
  # Create a bash script that will execute our command and capture PIPESTATUS properly
  let bashScript: string = """
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
  var tmpFilename: string = getTempDir() / "yabb_XXXXXX"
  var fd: cint = posix.mkstemp(cstring(tmpFilename))
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
    var scriptCmd: ShellCommand = newShellCommand()
    scriptCmd.addArg(tmpFilename)
    let scriptResult: tuple[output: string, exitCode: int] = execShellCmd(scriptCmd)
    
    # Parse the output
    var 
      output: string = ""
      statuses: seq[int] = @[]
      inPipestatus: bool = false
      
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