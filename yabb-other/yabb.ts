// YABB - Yet Another BTRFS Backup
// This file contains only TypeScript/Deno code
// To run this script, use the companion yabb.sh script

/// <reference lib="deno.ns" />
import { retry } from "jsr:@std/async/retry";
import { deadline } from "jsr:@std/async";

import { ensureFile, exists} from "jsr:@std/fs";

import * as path from "jsr:@std/path";

import { 
  LimitedBytesTransformStream,
  mergeReadableStreams,
  TextDelimiterStream,
  TextLineStream,
  toText,
} from "jsr:@std/streams";
import { FixedChunkStream } from "jsr:@std/streams/unstable-fixed-chunk-stream";

import { setup, getLogger, ConsoleHandler } from "jsr:@std/log";
import type { LogRecord, LogConfig } from "jsr:@std/log";

// ==================== Configuration ====================
const config = {
  sourceVol: "/data",
  snapDir: "/data/.snapshots",
  destMount: "/mnt/external",
  minFreeGb: 1,
  lockFile: "/var/lock/external-backup.lock",
  showProgressPercent: true,
  lockFileMode: 0o600,
} as const satisfies Readonly<{
  sourceVol: string;
  snapDir: string;
  destMount: string;
  minFreeGb: number;
  lockFile: string;
  showProgressPercent: boolean;
  lockFileMode: number;
}>;

// ==================== Constants ====================
const ERROR_MESSAGES = {
  ELOCK: "Failed to acquire lock file",
  EMOUNT: "Mount point operation failed",
  ESNAPSHOT: "Snapshot operation failed",
  ESPACE: "Insufficient disk space",
  EDEPENDENCY: "Missing required dependency",
  EUUID: "UUID mismatch detected",
  ECOMMAND: "Command execution failed",
  EINVALID: "Invalid input or configuration",
  ETRANSID: "Transaction ID mismatch or parsing failed",
  ETEMP: "Temporary file operation failed",
  ETIMEDOUT: "Operation timed out",
  EWRITE: "Failed to write data to storage",
} as const;

const Color = {
  red: (text: string) => `\x1b[31m${text}\x1b[0m`,
  green: (text: string) => `\x1b[32m${text}\x1b[0m`,
  yellow: (text: string) => `\x1b[33m${text}\x1b[0m`,
  cyan: (text: string) => `\x1b[36m${text}\x1b[0m`,
  gray: (text: string) => `\x1b[90m${text}\x1b[0m`,
  bold: (text: string) => `\x1b[1m${text}\x1b[22m`,
};

// ==================== Interfaces & Types ====================
// Add logger types here with other type definitions
type LoggerMethod = (message: string, ...args: unknown[]) => void;

interface AppLogger {
  debug: LoggerMethod;
  info: LoggerMethod;
  warn: LoggerMethod;
  error: LoggerMethod;
  critical: LoggerMethod;
}

interface ErrorContext extends Readonly<Record<string, unknown>> {
  readonly path?: string;
  readonly command?: string[];
  readonly exitCode?: number;
  readonly suggestions?: readonly string[];
  readonly recommendation?: string;
}

interface AppConfig {
  readonly jsonOutput: boolean;
  readonly colorOutput: boolean;
  readonly showProgress: boolean;
  readonly destMount: string;
  readonly snapDir: string;
  readonly sourceVol: string;
  readonly devicePath?: string;
}

// ==================== Logger Configuration ====================
// Initialize typed logger after config but before first use
const LOG_CONFIG: LogConfig = {
  handlers: {
    console: new ConsoleHandler("DEBUG", {
      formatter: (logRecord: LogRecord) => {
        let message;
        try {
          message = logRecord.args[0] === undefined || logRecord.args.length === 0
            ? "[No message]"
            : typeof logRecord.args[0] === 'string'
              ? logRecord.args[0]
              : Deno.inspect(logRecord.args[0]);
        } catch (_error) {
          message = "Error formatting log message";
        }
        return `${logRecord.datetime.toISOString()} [${logRecord.levelName}] ${message}`;
      },
      useColors: false,
    }),
  },
  loggers: {
    default: {
      level: "DEBUG",
      handlers: ["console"],
    },
  },
};

// Create typed logger instance after config
const logger: AppLogger = getLogger();

// ==================== Error Classes ====================
class BackupError extends Error {
  public readonly context?: ErrorContext;

  constructor(
    message: string,
    public readonly code: keyof typeof ERROR_MESSAGES,
    options?: ErrorOptions & { readonly context?: ErrorContext }
  ) {
    super(message, options);
    this.name = this.constructor.name;
    this.context = {
      pid: Deno.pid,
      timestamp: new Date().toISOString(),
      ...options?.context
    };
  }

  toJSON() {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      context: this.context,
      stack: this.stack,
    };
  }
}

class UuidMismatchError extends BackupError {
  constructor(
    message: string,
    public override readonly context: {
      sourceUuid?: string;
      destUuid?: string;
      sourcePath?: string;
      destPath?: string;
      recommendation?: string;
      cause?: unknown;
    } & ErrorContext
  ) {
    super(message, "EUUID", { 
      context,
      cause: context.cause
    });
  }

  override toJSON() {
    return {
      ...super.toJSON(),
      context: this.context
    };
  }
}

class PipelineError extends BackupError {
  constructor(
    message: string,
    public override readonly context: {
      readonly commands: Array<[string, string[]]>;
      readonly statuses: (Deno.CommandStatus | undefined)[];
      readonly stderrs: string[];
      readonly cause?: unknown;
      readonly suggestions?: readonly string[];
    }
  ) {
    super(message, "ECOMMAND", {
      context: {
        ...context,
        suggestions: [
          ...(context.suggestions || []),
          "Check individual command exit codes",
          "Verify pipeline connectivity",
          "Review command-specific error outputs"
        ]
      }
    });
  }

  override toJSON() {
    return {
      ...super.toJSON(),
      failedCommands: this.context.commands
        .map(([cmd, args], index) => ({
          command: cmd,
          args,
          exitCode: this.context.statuses[index]?.code,
          stderrSnippet: this.context.stderrs[index]?.slice(0, 200)
        }))
        .filter(cmd => cmd.exitCode !== 0)
    };
  }
}

// ==================== Core Classes ====================
class BackupState {
  private static instance: BackupState;
  private constructor(
    public snapshotCreated = false,
    public backupSuccessful = false,
    public snapshotName = "",
    public readonly srcUuid = "",
    public readonly destUuid = "",
    public readonly tempDir?: string,
    public readonly tempPathFile?: string
  ) {}

  static getInstance(): BackupState {
    return this.instance ??= new BackupState();
  }

  with(values: Partial<BackupState>): BackupState {
    const newState = new BackupState(
      values.snapshotCreated ?? this.snapshotCreated,
      values.backupSuccessful ?? this.backupSuccessful,
      values.snapshotName ?? this.snapshotName,
      values.srcUuid ?? this.srcUuid,
      values.destUuid ?? this.destUuid,
      values.tempDir ?? this.tempDir,
      values.tempPathFile ?? this.tempPathFile
    );
    BackupState.instance = newState;
    return newState;
  }

  static async createTempResources(): Promise<BackupState> {
    const tempDir = await Deno.makeTempDir({ prefix: "yabb_" });
    const tempPathFile = path.join(tempDir, "paths.json");
    await ensureFile(tempPathFile);
    await Deno.writeTextFile(tempPathFile, "[]");

    // Update singleton with new temp resources
    const newState = new BackupState(
      this.instance?.snapshotCreated ?? false,
      this.instance?.backupSuccessful ?? false,
      this.instance?.snapshotName ?? "",
      this.instance?.srcUuid ?? "",
      this.instance?.destUuid ?? "",
      tempDir,
      tempPathFile
    );
    
    BackupState.instance = newState;
    return newState;
  }
}

// ==================== Error Formatter ====================
class ErrorFormatter {
  constructor(private readonly config: AppConfig) {}

  private colorize(text: string, colorFn: (text: string) => string): string {
    return this.config.colorOutput ? colorFn(text) : text;
  }

  format(error: unknown): string {
    if (this.config.jsonOutput) {
      return JSON.stringify(
        error, 
        (_key, value) => {
          if (value instanceof Error) return this.serializeError(value);
          return value;
        },
        2
      );
    }
    return this.formatText(error);
  }

  private serializeError(error: Error): object {
    return error instanceof BackupError 
      ? error.toJSON()
      : { name: error.name, message: error.message, stack: error.stack };
  }

  private formatUuidError(error: UuidMismatchError): string[] {
    const fallback = (uuid?: string) => uuid || "N/A";
    return [
      `Source UUID: ${fallback(error.context.sourceUuid)}`,
      `Dest UUID:   ${fallback(error.context.destUuid)}`
    ];
  }

  private formatText(error: unknown, depth = 0): string {
    const indent = "  ".repeat(depth);
    const lines: string[] = [];
    
    if (!error) return `${indent}Unknown error occurred`;

    if (error instanceof BackupError) {
      lines.push(
        this.colorize(`${indent}${error.name} [${error.code}]`, Color.red),
        `${indent}${this.colorize("Description:", Color.cyan)} ${ERROR_MESSAGES[error.code]}`
      );

      if (error.context) {
        lines.push(`${indent}${this.colorize("Context:", Color.cyan)}`);
        
        for (const [key, value] of Object.entries(error.context)) {
          if (key === 'suggestions' || key === 'recommendation') continue;
          lines.push(`${indent}  ${this.colorize(key + ":", Color.gray)} ${Deno.inspect(value)}`);
        }

        if (error.context.suggestions?.length) {
          lines.push(
            `${indent}${this.colorize("Suggested Actions:", Color.green)}`,
            ...error.context.suggestions.map(s => 
              `${indent}  • ${this.colorize(s, Color.gray)}`
            )
          );
        }

        if (error.context.recommendation) {
          lines.push(
            `${indent}${this.colorize("Recommendation:", Color.green)}`,
            `${indent}  ${this.colorize(error.context.recommendation, Color.gray)}`
          );
        }
      }

      if (error.stack) {
        lines.push(
          `${indent}${this.colorize("Stack:", Color.cyan)}`,
          ...error.stack.split("\n").map(l => 
            this.colorize(`${indent}  ${l.trim()}`, Color.gray)
          )
        );
      }
    } else if (error instanceof Error) {
      lines.push(
        this.colorize(`${indent}${error.name}`, Color.red),
        `${indent}${this.colorize("Message:", Color.yellow)} ${error.message}`
      );
    }

    if (error instanceof Error && error.cause) {
      lines.push(
        `\n${indent}${this.colorize("Caused by:", Color.cyan)}`,
        this.formatText(error.cause, depth + 1)
      );
    }

    if (error instanceof UuidMismatchError) {
      lines.push(
        this.colorize("UUID Mismatch Details:", Color.red),
        ...this.formatUuidError(error).map(l => `${indent}  ${l}`)
      );
    }

    return lines.join("\n");
  }

  static parse(json: string): unknown {
    return JSON.parse(json, errorReviver);
  }
}

// ==================== Error Serialization Utilities ====================
interface SerializedUuidError {
  code: "EUUID";
  message: string;
  context: {
    sourceUuid?: string;
    destUuid?: string;
    sourcePath?: string;
    destPath?: string;
    recommendation?: string;
  } & ErrorContext;
}

function isSerializedUuidError(value: unknown): value is SerializedUuidError {
  return typeof value === "object" && 
    value !== null &&
    "code" in value && 
    (value as { code: unknown }).code === "EUUID" &&
    "context" in value &&
    typeof (value as { context: unknown }).context === "object";
}

const errorReviver = (_key: string, value: unknown): unknown => {
  if (isSerializedUuidError(value)) {
    return new UuidMismatchError(value.message, value.context);
  }
  return value;
};

// ==================== Utility Functions ====================
const formatBytes = (bytes: number): string => {
  const units = ["B", "KB", "MB", "GB", "TB"] as const;
  let unit = 0;
  
  for (; bytes >= 1024 && unit < units.length - 1; unit++, bytes /= 1024);
  
  return `${bytes.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
};

const convertToBytes = (sizeStr: string): number => {
  const match = sizeStr.match(/^(?<value>\d+(?:\.\d+)?)(?<unit>\D+)$/i);
  if (!match?.groups) throw new BackupError(`Invalid size format: ${sizeStr}`, "EINVALID");
  
  const factors = new Map<string, number>([
    ["B", 1],
    ["KB", 1024],
    ["MB", 1024 ** 2],
    ["GB", 1024 ** 3],
    ["TB", 1024 ** 4],
  ]);

  const unit = match.groups.unit.toUpperCase();
  const factor = factors.get(unit) ?? 1;
  return parseFloat(match.groups.value) * factor;
};

const sanitizeArgs = (args: string[]): string[] => 
  args.map(arg => arg.replace(/[^a-zA-Z0-9_\/\-=.:\s]/g, ""));

// ==================== Command Execution ====================
const ALLOWED_COMMANDS = new Set([
  "btrfs",
  "mount",
  "mountpoint",
  "find",
  "pv",
  "du",
  "which",
  "test",
  "lsblk",
  "blkid"
] as const);

type AllowedCommand = typeof ALLOWED_COMMANDS extends Set<infer T> ? T : never;

const executeCommand = async (
  command: AllowedCommand,
  args: string[],
  options?: Deno.CommandOptions & { signal?: AbortSignal }
): Promise<{ output: Uint8Array; success: boolean }> => {
  const sanitizedArgs = sanitizeArgs(args);
  const signal = options?.signal || abortController.signal;

  if (!ALLOWED_COMMANDS.has(command)) {
    throw new BackupError(`Disallowed command: ${command}`, "EINVALID", {
      context: { attemptedCommand: [command, ...sanitizedArgs] },
    });
  }

  try {
    const cmd = new Deno.Command(command, { 
      args: sanitizedArgs,
      ...options,
      signal
    });

    const output = await cmd.output();
    
    if (!output.success) {
      throw new BackupError("Command execution failed", "ECOMMAND", {
        context: {
          command: [command, ...sanitizedArgs],
          exitCode: output.code,
          stderr: new TextDecoder().decode(output.stderr),
          ...(command === "btrfs" && args[0] === "subvolume" && args[1] === "show" 
            ? { potentialUuid: extractUuidFromOutput(output.stderr) }
            : {})
        }
      });
    }

    return { output: output.stdout, success: output.success };
  } catch (error) {
    let code: keyof typeof ERROR_MESSAGES = "ECOMMAND";
    let suggestions: readonly string[] = [];

    if (error instanceof Deno.errors.NotFound) {
      code = "EDEPENDENCY";
      suggestions = [`Install missing dependency: ${command}`];
    } else if (error instanceof Deno.errors.PermissionDenied) {
      code = "EINVALID";
      suggestions = ["Check execution permissions"];
    } else if (error instanceof Deno.errors.InvalidData) {
      code = "ETRANSID";
      suggestions = ["Verify input data format"];
    } else if (error instanceof Deno.errors.Interrupted) {
      code = "ESNAPSHOT";
      suggestions = [
        "Retrying operation...",
        "Check system stability if frequent interruptions occur"
      ];
    }

    // Then create context with frozen suggestions
    const context: ErrorContext = { 
      command: [command, ...sanitizedArgs],
      suggestions: Object.freeze([...suggestions])
    };

    throw new BackupError("Command execution failed", code, {
      cause: error,
      context
    });
  }
};

// ==================== New Progress Types ====================
interface ProgressStats {
  bytesTransferred: number;
  throughput: string;
  percentage: number;
  elapsed: string;
  eta: string;
}

// ==================== Global Signal Handling ====================
const abortController = new AbortController();

// Named handler functions
const signalHandlers = {
  SIGINT: () => abortController.abort("SIGINT received"),
  SIGTERM: () => abortController.abort("SIGTERM received"),
  SIGHUP: () => abortController.abort("SIGHUP received")
};

// ==================== Updated Process Execution ====================
const executePipeline = async (
  commands: Array<[string, string[]]>,
  sizeBytes?: number,
  showProgress: boolean = true
): Promise<void> => {
  const processes = commands.map(([cmd, args]) => {
    const isBtrfsSend = cmd === "btrfs" && args[0] === "send";
    const isBtrfsReceive = cmd === "btrfs" && args[0] === "receive";
    
    const childProcess = new Deno.Command(cmd, {
      args: [
        ...(isBtrfsSend ? ["-e", "1024"] : []), // Add chunk size for send
        ...args,
        ...(isBtrfsSend && sizeBytes ? ["-s", sizeBytes.toString()] : []),
        ...(isBtrfsSend && showProgress ? ["-p"] : [])
      ],
      stdin: "piped",
      stdout: "piped",
      stderr: "piped", // Always pipe stderr for all processes
      signal: abortController.signal
    }).spawn();

    return {
      childProcess,
      wrappedStreams: {
        stdout: isBtrfsSend 
          ? childProcess.stdout.pipeThrough(new FixedChunkStream(1024 * 1024)) // 1MB chunks
          : childProcess.stdout,
        stdin: childProcess.stdin,
        stderr: childProcess.stderr,
        status: childProcess.status
      },
      isBtrfsSend,
      isBtrfsReceive
    };
  });

  try {
    // Connect pipeline using ReadableStream composition for stdout
    const pipeline = processes.reduce((prev, { wrappedStreams }) => 
      prev.pipeThrough({
        readable: wrappedStreams.stdout,
        writable: wrappedStreams.stdin
      }), 
      new ReadableStream() as ReadableStream<Uint8Array>
    );

    // Add progress monitoring here
    const pvProcess = processes.find(p => p.isBtrfsSend);
    if (pvProcess && showProgress) {
      readProgress(pvProcess.wrappedStreams.stderr, showProgress);
    }

    // Collect error streams using ReadableStream
    const stderrPromises = processes.map(async ({ wrappedStreams }) => {
      try {
        return await toText(wrappedStreams.stderr);
      } catch (_error) {
        return ""; // Return empty string if stderr isn't available
      }
    });

    // Combined stderr handling like the shell script
    const stderrStreams = processes.map(p => p.wrappedStreams.stderr);
    const combinedStderr = mergeReadableStreams(...stderrStreams)
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(new TextLineStream())
      .pipeThrough(new TransformStream<string, string>({
        transform(line, controller) {
          // Filter out "write ... offset=" lines like the shell script does
          if (!line.match(/write\s+.*\soffset=/)) {
            controller.enqueue(line + "\n");
          }
        }
      }))
      .pipeThrough(new TextEncoderStream());

    // Pipe combined stderr to stderr
    combinedStderr.pipeTo(Deno.stderr.writable).catch(() => {
      /* Ignore stderr pipe errors */
    });

    // Add abortable writer
    const writer = Deno.stdout.writable.getWriter();
    const abortableWriter = new WritableStream({
      write(chunk) {
        return writer.write(chunk);
      },
      // Don't close the underlying stdout resource
      close() {
        // Just release the writer without closing
        writer.releaseLock();
      },
      abort(reason) {
        writer.abort(reason);
        writer.releaseLock();
      }
    });

    // Execute pipeline
    await pipeline.pipeTo(abortableWriter, { 
      signal: abortController.signal 
    }).catch(error => {
      if (error instanceof Deno.errors.BadResource) {
        throw new PipelineError("Stream resource error - possible premature closure", {
          commands,
          statuses: [],
          stderrs: [],
          cause: error,
          suggestions: [
            "Check storage device stability",
            "Verify network connection if using remote storage",
            "Retry with --no-progress flag"
          ]
        });
      }
      throw error;
    });

    // Check command statuses after pipeline completes
    const statuses = await Promise.all(processes.map(p => 
      deadline(p.wrappedStreams.status, 300_000) // 5m timeout
    ));
    
    const stderrs = await Promise.all(stderrPromises);

    const failedCommands = statuses
      .map((status, index) => ({ status, command: commands[index] }))
      .filter(({ status }) => !status.success);

    if (failedCommands.length > 0) {
      throw new PipelineError("Pipeline command failed", {
        commands,
        statuses,
        stderrs,
        cause: failedCommands[0].status.code,
        suggestions: [
          "Verify source data integrity with 'btrfs scrub'",
          "Check storage device health",
          "Retry the operation"
        ]
      });
    }

  } catch (error) {
    // Handle errors similarly to before
    if (error instanceof BackupError) throw error;
    
    // Get as many statuses as possible
    const partialStatuses = await Promise.all(
      processes.map(p => p.wrappedStreams.status.catch(() => undefined))
    );
    
    // Get as many stderrs as possible
    const partialStderrs = await Promise.all(
      processes.map(async (p) => {
        try {
          return await toText(p.wrappedStreams.stderr);
        } catch (_error) {
          return "";
        }
      })
    );

    // Create appropriate error based on type
    throw new PipelineError(
      error instanceof Deno.errors.Interrupted ? "Pipeline interrupted mid-execution" :
      error instanceof Deno.errors.UnexpectedEof ? "Unexpected end of data stream" :
      error instanceof Deno.errors.WriteZero ? "Data write failure - zero bytes written" :
      "Pipeline execution failed", 
      {
        commands,
        statuses: partialStatuses,
        stderrs: partialStderrs,
        cause: error,
        suggestions: [
          "Verify source data integrity with 'btrfs scrub'",
          "Check storage device health",
          "Retry the operation"
        ]
      }
    );
  } finally {
    if(abortController.signal.aborted) {
      processes.forEach(p => {
        // Abort all writable streams in the pipeline
        p.wrappedStreams.stdin.abort("Pipeline aborted").catch(() => {});
        p.childProcess.kill();
      });
    }
  }
};

// ==================== Updated Retry Logic ====================
const retryOperation = async <T>(
  operation: (signal: AbortSignal) => Promise<T>,
  maxAttempts: number,
  baseDelayMs: number,
): Promise<T> => {
  const controller = new AbortController();
  const { signal } = controller;
  
  return await retry(async () => {
    try {
      if (signal.aborted) throw new DOMException("Aborted", "AbortError");
      return await operation(signal);
    } catch (error) {
      if (error instanceof Deno.errors.Interrupted) {
        controller.abort();
      }
      throw error;
    }
  }, {
    maxAttempts,
    minTimeout: baseDelayMs,
    maxTimeout: baseDelayMs * 4, // Exponential backoff cap
    jitter: 0.5
  });
};

// ==================== Updated Progress Monitoring ====================
const readProgress = async (
  stderr: ReadableStream<Uint8Array>, 
  showProgress: boolean
) => {
  const decoder = new TextDecoderStream();
  const lineStream = new TextDelimiterStream("\r");
  
  const writer = new WritableStream({
    write: (chunk) => {
      const stats = parsePvOutput(chunk);
      if(stats) updateProgressDisplay(stats, showProgress);
    }
  });

  try {
    await stderr
      .pipeThrough(decoder)
      .pipeThrough(lineStream)
      .pipeTo(writer, { signal: abortController.signal });
  } catch (error) {
    if (error instanceof Deno.errors.UnexpectedEof) {
      userFeedback.warning("Progress stream ended unexpectedly", parseConfig());
    }
    if(abortController.signal.aborted) {
      writer.abort("Progress monitoring aborted");
    }
  }
};

const parsePvOutput = (text: string): ProgressStats | null => {
  // Single-line regex with proper escaping
  const match = text.match(
    /^\s*(?<bytes>[0-9.]+)(?<unit>[A-Za-z]+)\s+(?<elapsed>\d+:\d{2}:\d{2})\s+\[(?<throughput>[0-9.]+[A-Za-z]+\/s)\]\s+\[(?<bar>[=>-]+)\]\s+(?<percent>\d+)%\s+ETA\s+(?<eta>\d+:\d{2}:\d{2})/
  );

  if (!match?.groups) return null;

  return {
    bytesTransferred: convertToBytes(`${match.groups.bytes}${match.groups.unit}`),
    throughput: match.groups.throughput,
    percentage: parseInt(match.groups.percent, 10),
    elapsed: match.groups.elapsed,
    eta: match.groups.eta
  };
};

const updateProgressDisplay = (stats: ProgressStats, showProgress: boolean) => {
  if (showProgress) {
    const progressBar = createProgressBar(stats.percentage);
    logger.info(
      `\rProgress: ${progressBar} ${stats.percentage}% | ` +
      `${formatBytes(stats.bytesTransferred)} | ${stats.throughput} | ` +
      `Elapsed: ${stats.elapsed} | ETA: ${stats.eta}`
    );
  } else {
    logger.info(
      `\rTransfer: ${formatBytes(stats.bytesTransferred)} | ` +
      `${stats.throughput} | Elapsed: ${stats.elapsed}`
    );
  }
};

const createProgressBar = (percent: number): string => {
  const width = 20;
  const filled = Math.round(width * (percent / 100));
  return Color.green('[') + 
    Color.yellow('='.repeat(filled)) + 
    Color.gray('-'.repeat(width - filled)) + 
    Color.green(']');
};

// ==================== Updated Backup Operations ====================
const getSnapName = (): string => {
  const now = new Date();
  const pad = (n: number) => n.toString().padStart(2, '0');
  return `${path.basename(config.sourceVol)}.${now.getUTCFullYear()}-` +
    `${pad(now.getUTCMonth()+1)}-${pad(now.getUTCDate())}T` +
    `${pad(now.getUTCHours())}:${pad(now.getUTCMinutes())}:${pad(now.getUTCSeconds())}Z`;
};

const estimateDeltaSize = async (
  parentPath: string,
  currentPath: string
): Promise<number> => {
  const validatePath = (p: string) => {
    const absPath = path.isAbsolute(p) ? p : path.resolve(p);
    const commonPath = path.common([config.sourceVol, config.destMount]);
    
    if (path.common([absPath, commonPath]) !== commonPath) {
      throw new BackupError("Invalid path for estimation", "EINVALID", {
        context: {
          parentPath: path.format(path.parse(parentPath)),
          currentPath: path.format(path.parse(currentPath)),
          suggestions: ["Use paths within backup volumes"]
        }
      });
    }
  };

  try {
    const dryRunStream = new ReadableStream<Uint8Array>({
      async start(controller) {
        const process = new Deno.Command("btrfs", {
          args: ["send", "-p", parentPath, "--no-data", currentPath],
          stdout: "piped",
          stderr: "piped",
          signal: abortController.signal
        }).spawn();

        // Create proper writable stream for the controller
        const writable = new WritableStream<Uint8Array>({
          write(chunk) {
            controller.enqueue(chunk);
          },
          close() {
            controller.close();
          },
          abort(reason) {
            controller.error(reason);
          }
        });

        try {
          await process.stdout
            .pipeThrough(new LimitedBytesTransformStream(10_485_760, { error: true }))
            .pipeTo(writable);
        } catch (error) {
          if (error instanceof RangeError) {
            controller.close();
          } else {
            throw error;
          }
        }
      }
    });

    // Use TransformStream to collect bytes
    let totalBytes = 0;
    const byteCounter = new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        totalBytes += chunk.byteLength;
        controller.enqueue(chunk);
      }
    });

    await dryRunStream
      .pipeThrough(byteCounter)
      .pipeTo(new WritableStream()); // Drain the stream

    return Math.max(Math.floor(totalBytes * 1.05), 10_485_760);
  } catch (_error) {
    const config = parseConfig();
    userFeedback.warning("Dry-run estimation failed", config);
    userFeedback.warning("Using fallback method...", config);
  }

  try {
    validatePath(currentPath);
    const duResult = await executeCommand("du", ["-sb", currentPath], { signal: abortController.signal });
    const duOutput = new TextDecoder().decode(duResult.output);
    const sizeMatch = duOutput.match(/^(?<bytes>\d+)\s+/);
    
    if (!sizeMatch?.groups?.bytes) {
      throw new BackupError("Failed to parse du output", "EINVALID", {
        context: { duOutput }
      });
    }

    const subvolBytes = Number(sizeMatch.groups.bytes);
    if (isNaN(subvolBytes)) {
      throw new BackupError("Invalid byte count from du", "EINVALID", {
        context: { parsedValue: sizeMatch.groups.bytes }
      });
    }

    return Math.max(
      Math.floor(subvolBytes * 0.1 * 1.05),
      10_485_760
    );
  } catch (error) {
    if (error instanceof BackupError) throw error;
    throw new BackupError("Space estimation failed", "ESPACE", {
      cause: error,
      context: {
        parentPath: path.toNamespacedPath(parentPath),
        currentPath: path.toNamespacedPath(currentPath)
      }
    });
  }
};

// ==================== Interactive Feedback ====================
const userFeedback = {
  info: (message: string, config: AppConfig) => {
    logger.info(config.colorOutput ? Color.cyan(message) : message);
  },
  warning: (message: string, config: AppConfig) => {
    logger.warn(config.colorOutput ? Color.yellow(message) : message);
  },
  success: (message: string, config: AppConfig) => {
    logger.info(config.colorOutput ? Color.green(message) : message);
  },
  progress: (message: string, config: AppConfig) => {
    if (config.showProgress && !config.jsonOutput) {
      logger.info(message);
    }
  },
  error: (message: string, config: AppConfig) => {
    const formatter = new ErrorFormatter(config);
    logger.error(formatter.format(message));
  }
};

const checkDestinationSpace = async (requiredBytes: number, config: AppConfig): Promise<void> => {
  await retryOperation(async () => {
    userFeedback.info(`Checking destination free space on ${config.destMount}...`, config);

    const bufferBytes = 1_073_741_824; // 1GB buffer
    const requiredWithBuffer = requiredBytes + bufferBytes;

    const fsUsage = await executeCommand("btrfs", [
      "filesystem", "usage", "-b", config.destMount
    ], { signal: abortController.signal });
    
    const usageOutput = new TextDecoder().decode(fsUsage.output);
    
    // Match the "Free (estimated)" line directly with regex
    const freeMatch = usageOutput.match(/Free \(estimated\):\s+(\d+)/);
    
    if (!freeMatch || !freeMatch[1]) {
      throw new BackupError("Failed to parse btrfs output", "EINVALID", {
        context: { 
          usageOutput: usageOutput.slice(0, 500),
          suggestions: ["Check if btrfs filesystem usage output format has changed"]
        }
      });
    }

    const freeBytes = Number(freeMatch[1]);
    if (isNaN(freeBytes)) {
      throw new BackupError("Invalid free space value", "EINVALID", {
        context: {
          parsedValue: freeMatch[1],
          suggestions: ["Check btrfs filesystem usage output format"]
        }
      });
    }

    if (freeBytes < requiredWithBuffer) {
      throw new BackupError("Insufficient destination space", "ESPACE", {
        context: {
          required: formatBytes(requiredWithBuffer),
          available: formatBytes(freeBytes),
          buffer: formatBytes(bufferBytes)
        }
      });
    }

    userFeedback.info(
      `Space check passed - ${formatBytes(freeBytes)} available (needed ${formatBytes(requiredWithBuffer)})`,
      config
    );
  }, 2, 3000);
};

const createSnapshot = async (): Promise<void> => {
  const snapName = getSnapName();
  const snapPath = path.join(config.snapDir, snapName);
  const state = BackupState.getInstance(); // Get state BEFORE retries
  
  try {
    await retryOperation(async () => {
      await executeCommand("btrfs", [
        "subvolume", "snapshot", "-r", 
        config.sourceVol, 
        snapPath
      ], { signal: abortController.signal });
    }, 3, 5000);

    // Update state AFTER successful retry loop
    state.with({ 
      snapshotCreated: true,
      snapshotName: snapName
    });
    
    userFeedback.success(`Created snapshot ${snapPath}`, parseConfig());
  } catch (error) {
    // Clear state if snapshot creation ultimately failed
    state.with({ snapshotCreated: false, snapshotName: "" });
    throw new BackupError("Snapshot creation failed", "ESNAPSHOT", {
      cause: error,
      context: { path: snapPath }
    });
  }
};

async function findParentSnapshot(): Promise<string | null> {
  try {
    // Get the ACTUAL snapshot name from state instead of generating a new one
    const state = BackupState.getInstance();
    const currentSnap = state.snapshotName; // Use stored name instead of getSnapName()
    
    const sourceBaseName = path.basename(config.sourceVol);
    const snapshots: Array<{ name: string; mtime: number }> = [];
    
    for await (const entry of Deno.readDir(config.snapDir)) {
      if (!entry.isDirectory || !entry.name.startsWith(sourceBaseName)) continue;
      if (entry.name === currentSnap) continue; // Now correctly skips actual current snapshot
      
      const stat = await Deno.stat(path.join(config.snapDir, entry.name));
      if (stat.mtime) snapshots.push({ name: entry.name, mtime: stat.mtime.getTime() });
    }

    if (snapshots.length === 0) {
      logger.info("No existing snapshots found in " + config.snapDir);
      return null;
    }

    snapshots.sort((a, b) => b.mtime - a.mtime);
    logger.info(`Found previous snapshots, selecting newest: ${snapshots[0].name}`);
    return snapshots[0].name;
  } catch (error) {
    throw new BackupError("Failed to find parent snapshot", "ESNAPSHOT", {
      cause: error,
      context: {
        snapDir: config.snapDir,
        sourceBaseName: path.basename(config.sourceVol),
        suggestions: ["Verify snapshot directory structure"]
      }
    });
  }
}

const performFullBackup = async (showProgress: boolean): Promise<void> => {
  const state = BackupState.getInstance();
  // Use the stored snapshot name instead of generating a new one
  const currentPath = path.join(config.snapDir, state.snapshotName);
  
  try {
    const sizeResult = await executeCommand("btrfs", [
      "subvolume", "show", currentPath
    ], { signal: abortController.signal });
    const sizeOutput = new TextDecoder().decode(sizeResult.output);
    const sizeMatch = sizeOutput.match(/Total bytes:\s+([\d,]+)/);
    
    const fullSize = sizeMatch?.[1] 
      ? Number(sizeMatch[1].replace(/,/g, "")) 
      : (await executeCommand("du", ["-sb", currentPath], { signal: abortController.signal })).output.byteLength;

    await checkDestinationSpace(fullSize, parseConfig());

    await executePipeline(
      [
        ["btrfs", ["send", currentPath]],
        ["pv", ["-etab"]],
        ["btrfs", ["receive", config.destMount]]
      ],
      fullSize,
      showProgress
    );
  } catch (error) {
    throw new BackupError("Full backup failed", "ESNAPSHOT", {
      cause: error,
      context: { path: currentPath }
    });
  }
};

const verifySubvolumeUuid = async (path: string): Promise<string> => {
  try {
    const showOutput = await executeCommand("btrfs", ["subvolume", "show", path], { signal: abortController.signal });
    const output = new TextDecoder().decode(showOutput.output);
    
    // Match more flexibly, similar to shell script's grep
    const uuidMatch = output.match(/UUID:\s+([0-9a-f-]{36})/i);
    if (!uuidMatch) throw new UuidMismatchError("Failed to parse source subvolume UUID", {
      cause: new Error("UUID pattern not found in subvolume output"),
      context: { path, outputSnippet: output.slice(0, 200) }
    });
    
    return uuidMatch[1];
  } catch (error) {
    throw new UuidMismatchError("Source UUID verification failed", {
      cause: error,
      context: { path }
    });
  }
};

const verifyReceivedUuid = async (subvolPath: string): Promise<string> => {
  try {
    const showOutput = await executeCommand("btrfs", ["subvolume", "show", subvolPath], { signal: abortController.signal });
    const output = new TextDecoder().decode(showOutput.output);
    
    // Match ONLY Received UUID - this is critical for correct comparison
    const receivedUuidMatch = output.match(/Received UUID:\s+([0-9a-f-]{36})/i);
    if (!receivedUuidMatch) throw new UuidMismatchError("No received UUID found in destination snapshot", {
      cause: new Error("Received UUID pattern not found in destination output"),
      context: { 
        path: path.toNamespacedPath(subvolPath),
        outputSnippet: output.slice(0, 200) 
      }
    });
    
    return receivedUuidMatch[1];
  } catch (error) {
    throw new UuidMismatchError("Destination UUID verification failed", {
      cause: error,
      context: { 
        path: path.toNamespacedPath(subvolPath)
      }
    });
  }
};

const verifyUuidMatch = async (sourcePath: string, destPath: string): Promise<void> => {
  const [srcUuid, destUuid] = await Promise.all([
    verifySubvolumeUuid(sourcePath),
    verifyReceivedUuid(destPath) 
  ]);

  if (srcUuid !== destUuid) {
    throw new UuidMismatchError(
      "Subvolume UUID mismatch - possible corruption detected",
      {
        sourceUuid: srcUuid,
        destUuid: destUuid,
        sourcePath,
        destPath,
        recommendation: "Verify backup integrity with 'btrfs scrub'"
      }
    );
  }
};

const verifySnapshotConsistency = async (
  sourcePath: string,
  destPath: string
): Promise<void> => {
  const config = parseConfig();
  
  try {
    // ONLY verify UUIDs match - don't check transaction IDs
    userFeedback.progress("Verifying subvolume UUIDs...", config);
    await verifyUuidMatch(sourcePath, destPath);
    
    // Skip transaction ID verification entirely - shell script doesn't do this
    // The shell script only cares about UUID matching for safety
  } catch (error) {
    if (error instanceof UuidMismatchError) throw error;
    
    throw new UuidMismatchError("Consistency verification failed", {
      cause: error,
      sourcePath,
      destPath
    });
  }
};

const performIncrementalBackup = async (parentSnap: string, showProgress: boolean) => {
  const config = parseConfig();
  userFeedback.progress(`Starting incremental backup from ${parentSnap}`, config);

  // Explicitly construct paths to preserve colons
  const parentPath = path.join(config.snapDir, parentSnap);
  const destParentPath = path.join(config.destMount, parentSnap);
  
  // Add explicit colon preservation verification
  const validatePathFormat = (p: string) => {
    if (!p.includes(':')) {
      throw new BackupError("Invalid snapshot path format", "EINVALID", {
        context: {
          path: p,
          suggestions: ["Verify snapshot naming convention with colons"]
        }
      });
    }
  };

  try {
    validatePathFormat(parentPath);
    validatePathFormat(destParentPath);

    if (!await exists(parentPath)) {
      throw new BackupError("Source parent snapshot not found", "ESNAPSHOT", {
        context: { path: parentPath }
      });
    }

    if (!await exists(destParentPath)) {
      throw new BackupError("Destination parent snapshot not found", "ESNAPSHOT", {
        context: { 
          path: destParentPath,
          suggestions: ["Perform full backup to establish baseline"]
        }
      });
    }

    // Verify parent exists and check consistency
    try {
      await executeCommand("btrfs", ["subvolume", "show", destParentPath], { signal: abortController.signal });
      await verifySnapshotConsistency(parentPath, destParentPath);
    } catch (error) {
      throw new BackupError("Parent snapshot verification failed", "ESNAPSHOT", {
        cause: error,
        context: { parentPath: destParentPath }
      });
    }

    const deltaSize = await estimateDeltaSize(parentPath, parentPath);
    await checkDestinationSpace(deltaSize, config);

    try {
      await verifyUuidMatch(parentPath, destParentPath);
      await executePipeline(
        [
          ["btrfs", ["send", "-p", parentPath, path.join(config.snapDir, BackupState.getInstance().snapshotName)]],
          ["pv", ["-etab"]],
          ["btrfs", ["receive", config.destMount]]
        ],
        deltaSize,
        showProgress
      );
      userFeedback.success(`Incremental backup completed successfully (${formatBytes(deltaSize)})`, config);
    } catch (error) {
      if (error instanceof UuidMismatchError) {
        userFeedback.warning(
          `UUID mismatch detected: ${error.context.sourceUuid} vs ${error.context.destUuid}`,
          parseConfig()
        );
      }
      userFeedback.warning(`Incremental backup failed after ${formatBytes(deltaSize)} transferred`, config);
      throw error;
    }
  } catch (error) {
    // Enhanced error context - create new error instead of mutating
    if (error instanceof BackupError) {
      throw new BackupError(error.message, error.code, {
        cause: error.cause,
        context: {
          ...error.context,
          sourcePath: parentPath,
          destPath: destParentPath,
          timestampFormat: "ISO 8601 with colons"
        }
      });
    }
    throw error;
  }
};

// ==================== Updated Core Functionality ====================
const withLock = async <T>(
  fn: (file: Deno.FsFile) => Promise<T>
): Promise<T> => {
  let file: Deno.FsFile | null = null;
  
  try {
    // Ensure lock file and directory exist with proper permissions
    try {
      await ensureFile(config.lockFile);
      await Deno.chmod(config.lockFile, config.lockFileMode);
    } catch (error) {
      throw new BackupError("Failed to initialize lock file", "ELOCK", {
        cause: error,
        context: {
          path: config.lockFile,
          mode: config.lockFileMode.toString(8),
          suggestions: [
            "Check directory permissions for: " + path.dirname(config.lockFile),
            "Verify filesystem has enough inodes",
            "Ensure parent directory exists"
          ]
        }
      });
    }

    file = await Deno.open(config.lockFile, {
      create: false,
      mode: config.lockFileMode,
      read: true,
      write: true,
    });

    // Create timeout signal and race against lock operation
    const timeoutSignal = AbortSignal.timeout(30_000);
    const lockPromise = file.lock(true); // Correct exclusive lock syntax
    
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutSignal.addEventListener("abort", () => 
        reject(new DOMException("Lock timeout", "TimeoutError"))
      );
    });

    await Promise.race([lockPromise, timeoutPromise]);
    
    return await fn(file);
  } catch (error) {
    if (error instanceof Deno.errors.Busy) {
      throw new BackupError("Lock file is already in use", "ELOCK", {
        cause: error,
        context: {
          suggestions: [
            "Another backup process might be running",
            "Check for stale lock file with 'ps aux | grep yabb'"
          ]
        }
      });
    }
    if (error instanceof DOMException && error.name === "TimeoutError") {
      throw new BackupError("Lock acquisition timed out", "ELOCK", {
        cause: error,
        context: {
          timeoutMs: 30_000,
          suggestions: ["Increase lock timeout in config"]
        }
      });
    }
    throw error;
  } finally {
    try {
      if (file) {
        await file.unlock().catch(() => {});
        file.close();
      }
      // Always attempt to remove lock file
      await Deno.remove(config.lockFile).catch(() => {});
    } catch (_error) {
      // Ignore any cleanup errors
    }
  }
};

// ==================== Updated Cleanup Process ====================
const cleanup = async (): Promise<void> => {
  const state = BackupState.getInstance();
  
  try {
    console.warn("[Cleanup] Starting cleanup process...");
    
    if (state.snapshotName && !state.backupSuccessful) {
      console.warn(`[Cleanup] Failed backup detected: ${state.snapshotName}`);
      
      try {
        // Check and clean up source snapshot with separate error handling
        const sourceSnapPath = path.join(config.snapDir, state.snapshotName);
        console.warn(`[Cleanup] Checking source snapshot: ${sourceSnapPath}`);
        const sourceExists = await exists(sourceSnapPath).catch(() => false);
        
        if (sourceExists) {
          console.warn(`[Cleanup] Removing source snapshot: ${sourceSnapPath}`);
          try {
            const result = await executeCommand("btrfs", ["subvolume", "delete", "--commit-after", sourceSnapPath], {
              signal: AbortSignal.timeout(5000)
            });
            console.warn(`[Cleanup] Source removal ${result.success ? 'succeeded' : 'failed'}`);
          } catch (error) {
            console.error(`[Cleanup] Source removal error caught: ${error instanceof Error ? error.message : String(error)}`);
          }
        } else {
          console.warn(`[Cleanup] Source snapshot doesn't exist: ${sourceSnapPath}`);
        }
      } catch (sourceError) {
        console.error(`[Cleanup] Source error: ${sourceError instanceof Error ? sourceError.message : String(sourceError)}`);
      }
      
      try {
        // Check and clean up destination snapshot with completely separate error handling
        const destSnapPath = path.join(config.destMount, state.snapshotName);
        console.warn(`[Cleanup] Checking destination snapshot: ${destSnapPath}`);
        const destExists = await exists(destSnapPath).catch(() => false);
        
        if (destExists) {
          console.warn(`[Cleanup] Removing destination snapshot: ${destSnapPath}`);
          try {
            const result = await executeCommand("btrfs", ["subvolume", "delete", "--commit-after", destSnapPath], {
              signal: AbortSignal.timeout(5000)
            });
            console.warn(`[Cleanup] Destination removal ${result.success ? 'succeeded' : 'failed'}`);
          } catch (error) {
            console.error(`[Cleanup] Destination removal error caught: ${error instanceof Error ? error.message : String(error)}`);
          }
        } else {
          console.warn(`[Cleanup] Destination snapshot doesn't exist: ${destSnapPath}`);
        }
      } catch (destError) {
        console.error(`[Cleanup] Destination error: ${destError instanceof Error ? destError.message : String(destError)}`);
      }
      
      return;
    }

    // Complete the emergency cleanup case with the same robust approach
    if (!state.snapshotName && !state.backupSuccessful) {
      console.warn("[Cleanup] Performing emergency cleanup scan");
      try {
        const newestSnapshot = await findNewestSnapshot();
        if (newestSnapshot) {
          console.warn(`[Cleanup] Found potential orphan: ${newestSnapshot}`);
          
          // Source cleanup
          try {
            const sourceSnapPath = path.join(config.snapDir, newestSnapshot);
            console.warn(`[Cleanup] Checking source snapshot: ${sourceSnapPath}`);
            const sourceExists = await exists(sourceSnapPath).catch(() => false);
            
            if (sourceExists) {
              console.warn(`[Cleanup] Removing orphaned source snapshot: ${sourceSnapPath}`);
              try {
                const result = await executeCommand("btrfs", ["subvolume", "delete", "--commit-after", sourceSnapPath], {
                  signal: AbortSignal.timeout(5000)
                });
                console.warn(`[Cleanup] Orphan source removal ${result.success ? 'succeeded' : 'failed'}`);
              } catch (error) {
                console.error(`[Cleanup] Orphan source removal error: ${error instanceof Error ? error.message : String(error)}`);
              }
            } else {
              console.warn(`[Cleanup] Orphaned source snapshot doesn't exist: ${sourceSnapPath}`);
            }
          } catch (sourceError) {
            console.error(`[Cleanup] Orphan source error: ${sourceError instanceof Error ? sourceError.message : String(sourceError)}`);
          }
          
          // Destination cleanup
          try {
            const destSnapPath = path.join(config.destMount, newestSnapshot);
            console.warn(`[Cleanup] Checking destination snapshot: ${destSnapPath}`);
            const destExists = await exists(destSnapPath).catch(() => false);
            
            if (destExists) {
              console.warn(`[Cleanup] Removing orphaned destination snapshot: ${destSnapPath}`);
              try {
                const result = await executeCommand("btrfs", ["subvolume", "delete", "--commit-after", destSnapPath], {
                  signal: AbortSignal.timeout(5000)
                });
                console.warn(`[Cleanup] Orphan destination removal ${result.success ? 'succeeded' : 'failed'}`);
              } catch (error) {
                console.error(`[Cleanup] Orphan destination removal error: ${error instanceof Error ? error.message : String(error)}`);
              }
            } else {
              console.warn(`[Cleanup] Orphaned destination snapshot doesn't exist: ${destSnapPath}`);
            }
          } catch (destError) {
            console.error(`[Cleanup] Orphan destination error: ${destError instanceof Error ? destError.message : String(destError)}`);
          }
        } else {
          console.warn("[Cleanup] No orphaned snapshots found");
        }
      } catch (error) {
        console.error(`[Cleanup] Emergency scan error: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  } catch (error) {
    console.error("[Cleanup Critical Error]", error instanceof Error ? error.stack : String(error));
  } finally {
    console.log("[Cleanup] Cleanup process completed");
  }
};

// Helper function for fallback scenario
async function findNewestSnapshot(): Promise<string | null> {
  const snapshots: Array<{name: string, mtime: number}> = [];
  
  for await (const entry of Deno.readDir(config.snapDir)) {
    if (entry.isDirectory && entry.name.startsWith(path.basename(config.sourceVol))) {
      const stat = await Deno.stat(path.join(config.snapDir, entry.name));
      if (stat.mtime) snapshots.push({ name: entry.name, mtime: stat.mtime.getTime() });
    }
  }
  
  return snapshots.sort((a, b) => b.mtime - a.mtime)[0]?.name || null;
}

// ==================== Validation & Verification ====================
const verifyDependencies = async (): Promise<void> => {
  const bins = ["btrfs", "pv", "mountpoint", "find"];
  
  await Promise.all(bins.map(async (bin) => {
    const binPath = `/usr/bin/${bin}`;
    const binExists = await exists(binPath, { 
      isFile: true, 
      isReadable: true 
    });
    
    if (!binExists) {
      userFeedback.error(`Missing required dependency: ${bin}`, parseConfig());
      throw new BackupError(`Missing dependency: ${bin}`, "EDEPENDENCY", {
        context: { 
          binPath,
          suggestions: [`Install package containing ${bin}`]
        }
      });
    }
  }));
};

const parseConfig = (): AppConfig => ({
  jsonOutput: Deno.args.includes("--json"),
  colorOutput: Deno.args.includes("--color") && Deno.stdout.isTerminal(),
  showProgress: Deno.stdout.isTerminal() && !Deno.args.includes("--no-progress"),
  destMount: path.normalize(config.destMount),
  snapDir: path.normalize(config.snapDir),
  sourceVol: path.normalize(config.sourceVol),
  devicePath: Deno.args.includes("--device") 
    ? Deno.args[Deno.args.indexOf("--device") + 1]
    : undefined
});

const extractUuidFromOutput = (output: Uint8Array): string | null => {
  const text = new TextDecoder().decode(output);
  return text.match(/UUID:\s+([0-9a-f-]{36})/i)?.[1] ?? null;
};

// ==================== Updated Mount Operations ====================
async function ensureMounted(mountPath: string, config: AppConfig): Promise<void> {
  const normalizedPath = path.normalize(mountPath);
  
  try {
    // First ensure mount point directory exists
    try {
      await Deno.mkdir(normalizedPath, { recursive: true });
      userFeedback.info(`Created mount point directory: ${normalizedPath}`, config);
    } catch (error) {
      if (!(error instanceof Deno.errors.AlreadyExists)) {
        throw new BackupError("Failed to create mount directory", "EMOUNT", {
          cause: error,
          context: {
            path: normalizedPath,
            suggestions: [
              "Check filesystem permissions",
              "Verify parent directory exists"
            ]
          }
        });
      }
    }

    // Then verify if already mounted
    userFeedback.progress(`Verifying mount point: ${normalizedPath}`, config);
    await retryOperation(async () => {
      await executeCommand("mountpoint", ["-q", normalizedPath], { signal: abortController.signal });
    }, 2, 2000);
    userFeedback.info(`Verified mount point: ${normalizedPath}`, config);
  } catch (mountError) {
    // If that fails, try device-based mount
    try {
      const devicePath = config.devicePath || await findMatchingDevice(normalizedPath);
      await executeCommand("mount", [devicePath, normalizedPath], { signal: abortController.signal });
      userFeedback.success(`Mounted device ${devicePath} to ${normalizedPath}`, config);
    } catch (deviceError) {
      throw new BackupError(`Mount operation failed for ${normalizedPath}`, "EMOUNT", {
        cause: new AggregateError([mountError, deviceError], "Multiple mount attempts failed"),
        context: {
          path: normalizedPath,
          attempts: [
            `Filesystem mount error: ${mountError instanceof Error ? mountError.message : String(mountError)}`,
            `Device mount error: ${deviceError instanceof Error ? deviceError.message : String(deviceError)}`
          ],
          suggestions: [
            "Connect storage device and try again",
            "Verify /etc/fstab entries",
            `Test manual mount: 'mount <device> ${normalizedPath}'`
          ]
        }
      });
    }
  }
}

interface BlockDevice {
  name: string;
  mountpoint: string | null;
  children?: BlockDevice[];
}

const findMatchingDevice = async (mountPath: string): Promise<string> => {
  try {
    const lsblk = await executeCommand("lsblk", ["-J", "-o", "NAME,MOUNTPOINT"]);
    const devices: { blockdevices: BlockDevice[] } = JSON.parse(new TextDecoder().decode(lsblk.output));
    
    const matchingDevice = devices.blockdevices.find((device: BlockDevice) => 
      device.mountpoint === path.normalize(mountPath)
    );

    if (matchingDevice) return `/dev/${matchingDevice.name}`;

    // Check filesystem labels
    const blkid = await executeCommand("blkid", ["-L", path.basename(mountPath)]);
    const labelOutput = new TextDecoder().decode(blkid.output);
    const labelDevice = labelOutput.split("\n")[0];
    if (labelDevice) return labelDevice;

    throw new Error("No matching device found");
  } catch (error) {
    throw new BackupError("Failed to find storage device", "EMOUNT", {
      cause: error,
      context: {
        mountPath,
        suggestions: [
          "Connect storage device and try again",
          "Verify /etc/fstab entries",
          `Test manual mount: 'mount <device> ${mountPath}'`
        ]
      }
    });
  }
};

// ==================== Modified Main Workflow ====================
const main = async () => {
  setup(LOG_CONFIG);
  const config = parseConfig();
  const state = BackupState.getInstance();

  // Register signal handlers when execution starts
  Deno.addSignalListener("SIGINT", signalHandlers.SIGINT);
  Deno.addSignalListener("SIGTERM", signalHandlers.SIGTERM);
  Deno.addSignalListener("SIGHUP", signalHandlers.SIGHUP);

  try {
    // Direct locking without separate verification
    await withLock(async () => {
      logger.info("Starting backup process");
      try {
        await verifyDependencies();
        logger.info("Dependencies verified successfully");
        
        await ensureMounted(config.sourceVol, config);
        logger.info(`Mounted source volume: ${config.sourceVol}`);
        
        await ensureMounted(config.destMount, config);
        logger.info(`Mounted destination: ${config.destMount}`);

        // Create new snapshot
        logger.info("Creating new snapshot...");
        await createSnapshot();
        // state.snapshotCreated is set to true in createSnapshot()

        // Determine backup type
        const parentSnap = await findParentSnapshot();
        const isFullBackup = parentSnap === null;

        // Perform backup
        if (isFullBackup) {
          logger.info("Starting full backup");
          await performFullBackup(config.showProgress);
        } else {
          logger.info(`Starting incremental backup from ${parentSnap}`);
          await performIncrementalBackup(parentSnap, config.showProgress);
        }

        // Only mark as successful if we get here
        state.with({ backupSuccessful: true });
        logger.info("Backup completed successfully");
      } catch (error) {
        // Explicitly ensure backupSuccessful is false WITHOUT changing other state
        // Do NOT reset snapshotCreated or snapshotName here
        const currentState = BackupState.getInstance();
        currentState.with({ backupSuccessful: false });
        
        const errMsg = error instanceof Error ? error.message : String(error);
        logger.error("Backup failed:", errMsg);
        throw error;
      }
    });
  } catch (error) {
    // Preserve snapshot info during error handling
    const currentState = BackupState.getInstance();
    
    // Set only backupSuccessful without resetting other state values
    currentState.with({ backupSuccessful: false });
    
    await cleanup();
    const formatter = new ErrorFormatter(parseConfig());
    logger.error("Backup process failed with error:\n" + formatter.format(error));
    Deno.exit(1);
  }
};

// Final verification of main error handler
main().catch((error: unknown) => {
  const formatter = new ErrorFormatter(parseConfig());
  try {
    logger.error("Fatal application error:\n" + formatter.format(error));
  } catch (loggerError) {
    console.error("Original error:", error);
    console.error("Logger failure:", formatter.format(loggerError));
  } finally {
    Deno.exit(1);
  }
});