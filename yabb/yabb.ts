#!/bin/sh
':' //
exec deno run \
  --allow-run=btrfs,mount,mountpoint,find,pv,du,which,test \
  --allow-read=/data,/mnt/external,/var/lock,/usr/bin,/etc/mtab \
  --allow-write=/data/.snapshots,/mnt/external,/var/lock \
  --allow-env=TZ \
  --allow-net=jsr.io \
  --allow-sys \
  --unstable-kv \
  --v8-flags="--max-old-space-size=256,--jitless,--optimize-for-size,--use-ic,--no-concurrent-recompilation,--enable-ssse3,--enable-sse4-1,--enable-sse4-2" \
  --no-check "$0" "$@"
exit $?

/// <reference lib="deno.ns" />
import { delay } from "jsr:@std/async/delay";
import { retry } from "jsr:@std/async/retry";
import { abortable, deadline } from "jsr:@std/async";

import { ensureFile, exists, expandGlob } from "jsr:@std/fs";

import * as path from "jsr:@std/path";

import { 
  LimitedBytesTransformStream,
  mergeReadableStreams,
  TextDelimiterStream,
  TextLineStream,
  toText,
} from "jsr:@std/streams";
import { FixedChunkStream } from "jsr:@std/streams/unstable-fixed-chunk-stream";

import { parse } from "jsr:@std/csv";

import { setup, getLogger, ConsoleHandler } from "jsr:@std/log";
import type { LogRecord, LevelName, LogConfig } from "jsr:@std/log";

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

const LOG_CONFIG: LogConfig = {
  handlers: {
    console: new ConsoleHandler("DEBUG", {
      formatter: (logRecord: LogRecord) => {
        let message;
        try {
          message = logRecord.args[0] === undefined
            ? "undefined"
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

// ==================== Interfaces & Types ====================
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
}

interface RetryContext extends ErrorContext {
  readonly retriesLeft?: number;
  readonly lastError?: unknown;
}

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
    this.context = options?.context;
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
    public readonly snapshotCreated = false,
    public readonly backupSuccessful = false,
    public readonly srcUuid = "",
    public readonly destUuid = "",
    public readonly tempDir?: string,
    public readonly tempPathFile?: string
  ) {}

  static getInstance(): BackupState {
    return this.instance ??= new BackupState();
  }

  with(values: Partial<BackupState>): BackupState {
    return new BackupState(
      values.snapshotCreated ?? this.snapshotCreated,
      values.backupSuccessful ?? this.backupSuccessful,
      values.srcUuid ?? this.srcUuid,
      values.destUuid ?? this.destUuid,
      values.tempDir ?? this.tempDir,
      values.tempPathFile ?? this.tempPathFile
    );
  }

  static async createTempResources(): Promise<BackupState> {
    const tempDir = await Deno.makeTempDir({ prefix: "yabb_" });
    const tempPathFile = path.join(tempDir, "paths.json");
    await ensureFile(tempPathFile);
    await Deno.writeTextFile(tempPathFile, "[]");

    return new BackupState(false, false, "", "", tempDir, tempPathFile);
  }
}

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
    
    if (!error) {
      return `${indent}Unknown error occurred`;
    }

    if (error instanceof BackupError) {
      lines.push(
        this.colorize(`${indent}${error.name} [${error.code}]`, Color.red),
        `${indent}${this.colorize("Description:", Color.cyan)} ${ERROR_MESSAGES[error.code]}`
      );

      if (error.context) {
        lines.push(`${indent}${this.colorize("Context:", Color.cyan)}`);
        
        // Display standard context fields
        for (const [key, value] of Object.entries(error.context)) {
          if (key === 'suggestions' || key === 'recommendation') continue;
          lines.push(`${indent}  ${this.colorize(key + ":", Color.gray)} ${Deno.inspect(value)}`);
        }

        // Display actionable suggestions
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

  // Add deserialization method
  static parseSerialized(json: string): unknown {
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
  args.map(arg => arg.replace(/[^a-zA-Z0-9_\/\-=.]/g, ""));

// ==================== Command Execution ====================
const ALLOWED_COMMANDS = new Set([
  "btrfs",
  "mount",
  "mountpoint",
  "find",
  "pv",
  "du",
  "which",
  "test"
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

// ==================== Updated Output Filtering ====================
const createOutputFilter = () => {
  const filterPattern = /write\s+.*\soffset=/;
  
  return new TransformStream<string, string>({
    transform(line, controller) {
      if (!filterPattern.test(line)) {
        controller.enqueue(line);
      }
    },
    flush(controller) {
      controller.terminate();
    }
  });
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
      stderr: isBtrfsSend ? "piped" : isBtrfsReceive ? "piped" : "inherit",
      signal: abortController.signal // Propagate abort signal
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
    // Connect pipeline using ReadableStream composition
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
      readProgress(pvProcess.wrappedStreams.stderr, showProgress, parseConfig());
    }

    // Collect error streams using ReadableStream
    const stderrPromises = processes.map(async ({ wrappedStreams }) => {
      if (!wrappedStreams.stderr) return "";
      return await toText(wrappedStreams.stderr);
    });

    const [statuses, stderrs] = await Promise.all([
      Promise.all(processes.map(p => 
        deadline(p.wrappedStreams.status, 300_000) // 5m timeout
      )),
      Promise.all(stderrPromises)
    ]);

    // Add abortable writer
    const writer = Deno.stdout.writable.getWriter();
    const abortableWriter = new WritableStream({
      write: (chunk) => writer.write(chunk),
      close: () => writer.close(),
      abort: (reason) => writer.abort(reason)
    });

    await pipeline.pipeTo(abortableWriter, { 
      signal: abortController.signal 
    });

    // Setup progress monitoring and filtering
    const receiveProcess = processes.find(p => p.isBtrfsReceive);
    if (receiveProcess) {
      const filteredStream = receiveProcess.wrappedStreams.stderr
        .pipeThrough(new TextDecoderStream())
        .pipeThrough(new TextLineStream({ allowCR: true }))
        .pipeThrough(createOutputFilter())
        .pipeThrough(new TextEncoderStream());

      const rawStream = receiveProcess.wrappedStreams.stderr;
      
      await mergeReadableStreams(rawStream, filteredStream)
        .pipeTo(Deno.stderr.writable)
        .catch(error => {
          throw new PipelineError("Output filtering failed", {
            commands,
            statuses,
            stderrs,
            cause: error,
            suggestions: [
              "Verify source data integrity with 'btrfs scrub'",
              "Check storage device health",
              "Retry the operation"
            ]
          });
        });
    }

    // Check command statuses after pipeline completes
    const failedCommands = statuses
      .map((status: Deno.CommandStatus | undefined, index: number) => ({ status, command: commands[index] }))
      .filter(({ status }: { status?: Deno.CommandStatus }) => !status?.success);

    if (failedCommands.length > 0) {
      throw new PipelineError("Pipeline command failed", {
        commands,
        statuses,
        stderrs,
        cause: failedCommands[0].status?.code,
        suggestions: [
          "Verify source data integrity with 'btrfs scrub'",
          "Check storage device health",
          "Retry the operation"
        ]
      });
    }

  } catch (error) {
    if (error instanceof BackupError) throw error;
    
    // Handle partial results if available
    const partialStatuses = await Promise.all(processes.map(p => p.wrappedStreams.status.catch(() => undefined)));
    const partialStderrs = await Promise.all(processes.map(async (p) => {
      if (!p.wrappedStreams.stderr) return "";
      return await toText(p.wrappedStreams.stderr);
    }));

    if (error instanceof Deno.errors.Interrupted) {
      throw new PipelineError("Pipeline interrupted mid-execution", {
        commands,
        statuses: partialStatuses,
        stderrs: partialStderrs,
        cause: error,
        suggestions: [
          "Verify source data integrity with 'btrfs scrub'",
          "Check storage device health",
          "Retry the operation"
        ]
      });
    }

    if (error instanceof Deno.errors.UnexpectedEof) {
      throw new PipelineError("Unexpected end of data stream", {
        commands,
        statuses: partialStatuses,
        stderrs: partialStderrs,
        cause: error,
        suggestions: [
          "Verify source data integrity with 'btrfs scrub'",
          "Check storage device health",
          "Retry the operation"
        ]
      });
    }

    if (error instanceof Deno.errors.WriteZero) {
      throw new PipelineError("Data write failure - zero bytes written", {
        commands,
        statuses: partialStatuses,
        stderrs: partialStderrs,
        cause: error,
        suggestions: [
          "Check destination storage device health",
          "Verify available disk space",
          "Test storage media with badblocks"
        ]
      });
    }

    throw new PipelineError("Pipeline execution failed", {
      commands,
      statuses: partialStatuses,
      stderrs: partialStderrs,
      cause: error,
      suggestions: [
        "Verify source data integrity with 'btrfs scrub'",
        "Check storage device health",
        "Retry the operation"
      ]
    });
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
  operation: (signal?: AbortSignal) => Promise<T>,
  maxRetries: number,
  retryDelayMs: number,
): Promise<T> => {
  const { signal } = abortController;
  
  return await retry(async () => {
    try {
      if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
      return await operation(signal);
    } catch (error) {
      if (error instanceof Deno.errors.Interrupted) {
        userFeedback.warning(`Retry attempt interrupted`, parseConfig());
      }
      throw error;
    }
  }, {
    maxAttempts: maxRetries + 1,
    minTimeout: retryDelayMs,
    jitter: 0.25,
    multiplier: 1,
  });
};

// ==================== Updated Progress Monitoring ====================
const readProgress = async (
  stderr: ReadableStream<Uint8Array>, 
  showProgress: boolean,
  config: AppConfig
) => {
  const decoder = new TextDecoderStream();
  const lineStream = new TextDelimiterStream("\r");
  
  const writer = new WritableStream({
    write: (chunk) => {
      const stats = parsePvOutput(chunk);
      if(stats) updateProgressDisplay(stats, showProgress, config);
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

const updateProgressDisplay = (stats: ProgressStats, showProgress: boolean, config: AppConfig) => {
  const logger = getLogger();
  if (showProgress) {
    const progressBar = createProgressBar(stats.percentage);
    logger.info(
      `\rProgress: ${progressBar} ${stats.percentage}% | ` +
      `${formatBytes(stats.bytesTransferred)} | ${stats.throughput} | ` +
      `Elapsed: ${stats.elapsed} | ETA: ${stats.eta}`,
      config,
      "PROGRESS" as LevelName
    );
  } else {
    logger.info(
      `\rTransfer: ${formatBytes(stats.bytesTransferred)} | ` +
      `${stats.throughput} | Elapsed: ${stats.elapsed}`,
      config,
      "PROGRESS" as LevelName
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

// ==================== Updated UnifiedErrorHandler ====================
// Delete this entire class declaration

// ==================== Updated Backup Operations ====================
const getSnapName = (): string => 
  `${path.basename(config.sourceVol)}.${new Date().toISOString()}`;

const removeSnapshot = async (
  basePath: string,
  signal?: AbortSignal
): Promise<void> => {
  const snapPath = path.join(basePath, getSnapName());
  await executeCommand("btrfs", ["subvolume", "delete", snapPath], { signal });
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
    const logger = getLogger();
    logger.info(config.colorOutput ? Color.cyan(message) : message);
  },
  warning: (message: string, config: AppConfig) => {
    const logger = getLogger();
    logger.warn(config.colorOutput ? Color.yellow(message) : message);
  },
  success: (message: string, config: AppConfig) => {
    const logger = getLogger();
    logger.info(config.colorOutput ? Color.green(message) : message);
  },
  progress: (message: string, config: AppConfig) => {
    if (config.showProgress && !config.jsonOutput) {
      const logger = getLogger();
      logger.info(message);
    }
  },
  error: (message: string, config: AppConfig) => {
    const logger = getLogger();
    logger.error(config.colorOutput ? Color.red(message) : message);
  }
};

const checkDestinationSpace = async (requiredBytes: number, config: AppConfig): Promise<void> => {
  userFeedback.info(`Checking destination free space on ${config.destMount}...`, config);

  const bufferBytes = 1_073_741_824;
  const requiredWithBuffer = requiredBytes + bufferBytes;

  try {
    const fsUsage = await executeCommand("btrfs", [
      "filesystem", "usage", "-b", config.destMount
    ], { signal: abortController.signal });
    
    const usageOutput = new TextDecoder().decode(fsUsage.output);
    const [, ...rows] = parse(usageOutput, {
      delimiter: " ",
      skipFirstRow: true,
      trimLeadingSpace: true,
      comment: "#"
    });

    const freeRow = rows.find(row => row[0] === "Free");
    if (!freeRow) {
      throw new BackupError("Failed to parse btrfs output", "EINVALID", {
        context: { usageOutput }
      });
    }

    const freeBytes = Number(freeRow?.[3] ?? "0");
    if (isNaN(freeBytes)) {
      throw new BackupError("Invalid free space value", "EINVALID", {
        context: {
          parsedValue: freeRow?.[3],
          rawRow: freeRow,
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
  } catch (error) {
    const message = error instanceof BackupError 
      ? error.message 
      : `Unexpected error: ${error instanceof Error ? error.message : String(error)}`;
    
    userFeedback.warning(`Space check failed: ${message}`, config);
    throw error;
  }
};

const createSnapshot = async (): Promise<void> => {
  const snapPath = path.join(config.snapDir, getSnapName());
  try {
    await executeCommand("btrfs", [
      "subvolume", "snapshot", "-r", 
      config.sourceVol, 
      snapPath
    ], { signal: abortController.signal });
    userFeedback.success(`Created snapshot ${snapPath}`, parseConfig());
  } catch (error) {
    throw new BackupError("Snapshot creation failed", "ESNAPSHOT", {
      cause: error,
      context: { path: snapPath }
    });
  }
};

const findParentSnapshot = async (): Promise<string | null> => {
  try {
    const pattern = `${path.basename(config.sourceVol)}.*`;
    const options = {
      root: config.snapDir,
      maxDepth: 1,
      exclude: [getSnapName()],
      includeDirs: true,
      extended: true,
    };

    let latestSnap: { path: string; mtime: Date } | null = null;
    for await (const entry of expandGlob(pattern, options)) {
      if (!entry.isDirectory) continue;
      const info = await Deno.stat(entry.path);
      const mtime = info.mtime ?? new Date(0); // Handle null mtime
      
      if (!latestSnap || mtime > latestSnap.mtime) {
        latestSnap = { path: entry.path, mtime };
      }
    }

    return latestSnap ? path.basename(latestSnap.path) : null;
  } catch (error) {
    throw new BackupError("Failed to find parent snapshot", "ESNAPSHOT", {
      cause: error,
      context: {
        snapDir: config.snapDir,
        pattern: `${path.basename(config.sourceVol)}.*`,
        suggestions: ["Verify snapshot directory structure"]
      }
    });
  }
};

const performFullBackup = async (showProgress: boolean): Promise<void> => {
  const currentPath = path.join(config.snapDir, getSnapName());
  
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
    
    // Match source UUID pattern
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
    
    const receivedUuidMatch = output.match(/Received\s+UUID:\s+([0-9a-f-]{36})/i);
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
        path: path.toNamespacedPath(subvolPath),
        formattedPath: path.format(path.parse(subvolPath))
      }
    });
  }
};

const verifyUuidMatch = async (sourcePath: string, destPath: string): Promise<void> => {
  const [srcUuid, destUuid] = await Promise.all([
    verifySubvolumeUuid(sourcePath),
    verifyReceivedUuid(destPath) // Changed to use received UUID check
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
    // First verify UUIDs
    userFeedback.progress("Verifying subvolume UUIDs...", config);
    await verifyUuidMatch(sourcePath, destPath);

    // Then verify transaction IDs
    userFeedback.progress("Verifying transaction IDs...", config);
    const sourceTid = await parseTransactionIdFromPath(sourcePath);
    const destTid = await parseTransactionIdFromPath(destPath);

    if (sourceTid !== destTid) {
      throw new UuidMismatchError("Transaction ID mismatch", {
        sourceTid,
        destTid,
        sourcePath,
        destPath
      });
    }
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

  const parentPath = path.join(config.snapDir, parentSnap);
  const currentPath = path.join(config.snapDir, getSnapName());
  const destParentPath = path.join(config.destMount, parentSnap);

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

  const deltaSize = await estimateDeltaSize(parentPath, currentPath);
  await checkDestinationSpace(deltaSize, config);

  try {
    await verifyUuidMatch(parentPath, destParentPath);
    await executePipeline(
      [
        ["btrfs", ["send", "-p", parentPath, currentPath]],
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
};

// ==================== Core Functionality ====================
const withLock = async <T>(
  fn: (file: Deno.FsFile) => Promise<T>
): Promise<T> => {
  const file = await Deno.open(config.lockFile, {
    create: true,
    mode: config.lockFileMode,
    read: true,
    write: true,
  });

  try {
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
    if (!abortController.signal.aborted) {
      await file.unlock().catch(() => {});
      file.close();
    }
  }
};

// ==================== Updated Cleanup Process ====================
const cleanup = async (): Promise<void> => {
  // Remove signal listeners first
  Deno.removeSignalListener("SIGINT", signalHandlers.SIGINT);
  Deno.removeSignalListener("SIGTERM", signalHandlers.SIGTERM);
  Deno.removeSignalListener("SIGHUP", signalHandlers.SIGHUP);
  
  const state = BackupState.getInstance();
  
  try {
    if (state.snapshotCreated && !state.backupSuccessful) {
      await Promise.allSettled([
        retryOperation(
          (signal) => removeSnapshot(config.snapDir, signal), 
          3, 
          1000
        ),
        retryOperation(
          (signal) => removeSnapshot(config.destMount, signal), 
          3, 
          1000
        )
      ]);
    }

    // Secure temp file cleanup
    if (state.tempDir || state.tempPathFile) {
      userFeedback.progress("Cleaning temporary resources...", parseConfig());
      const cleanupOps = [];
      
      if (state.tempPathFile) {
        cleanupOps.push(
          Deno.remove(state.tempPathFile).catch(e => {
            throw new BackupError("Failed to remove temporary file", "ETEMP", {
              cause: e
            });
          })
        );
      }
      
      if (state.tempDir) {
        cleanupOps.push(
          abortable(
            Deno.remove(state.tempDir, { recursive: true }),
            abortController.signal
          ).catch((e: unknown) => {
            if (e instanceof DOMException && e.name === "AbortError") return;
            throw new BackupError("Failed to remove temp dir", "ETEMP", { cause: e });
          })
        );
      }

      const results = await Promise.allSettled(cleanupOps);
      for (const result of results) {
        if (result.status === "rejected") {
          throw result.reason;
        }
      }
    }
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) {
      // Ignore missing files during cleanup
      return;
    }
    if (error instanceof Deno.errors.PermissionDenied) {
      throw new BackupError("Cleanup permission denied", "ETEMP", {
        context: {
          suggestion: "Run with appropriate permissions"
        }
      });
    }
    if (error instanceof Deno.errors.Interrupted) {
      userFeedback.warning("Cleanup interrupted, retrying...", parseConfig());
      await delay(500);
      return cleanup();
    }
    throw error;
  }
  
  try {
    await Deno.remove(config.lockFile);
  } catch (error) {
    if (!(error instanceof Deno.errors.NotFound)) {
      console.error("Failed to clean up lock file:", error);
    }
  }
};

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
      throw new BackupError(`Missing dependency: ${bin}`, "EDEPENDENCY", {
        context: { 
          binPath,
          suggestions: [
            `Install package containing ${bin}`,
            "Verify PATH environment variable"
          ]
        }
      });
    }
  }));
};

const verifyMountPoint = async (mountPath: string): Promise<void> => {
  const normalizedPath = path.normalize(mountPath);
  
  try {
    userFeedback.progress(`Verifying mount point: ${normalizedPath}`, parseConfig());
    await executeCommand("mountpoint", ["-q", normalizedPath], { signal: abortController.signal });
  } catch (error) {
    const parsedPath = path.parse(normalizedPath);
    throw new BackupError("Mount point verification failed", "EMOUNT", {
      cause: error,
      context: {
        action: 'verify',
        path: path.toNamespacedPath(normalizedPath),
        suggestions: [
          `Check mount status: mount | grep ${parsedPath.base}`,
          `Verify fstab entry for ${parsedPath.dir}`
        ]
      }
    });
  }
};

const parseTransactionIdFromPath = async (subvolPath: string): Promise<number> => {
  const normalizedPath = path.normalize(subvolPath);
  const showOutput = await executeCommand("btrfs", ["subvolume", "show", normalizedPath], { signal: abortController.signal });
  const output = new TextDecoder().decode(showOutput.output);

  try {
    const parsed = parse(output, {
      delimiter: ":",
      columns: ["key", "value"],
      trimLeadingSpace: true,
      comment: "#"
    });

    const tidRow = parsed.find(row => 
      row.key.toLowerCase().includes("transid") || 
      row.key.toLowerCase().includes("transaction id")
    );

    if (!tidRow?.value) {
      throw new BackupError("Transaction ID not found", "ETRANSID", {
        context: {
          outputSnippet: output.slice(0, 200),
          parsedFields: parsed.map(r => r.key),
          suggestions: ["Check btrfs subvolume show output format"]
        }
      });
    }

    const tid = parseInt(tidRow.value.trim(), 10);
    if (isNaN(tid)) {
      throw new BackupError("Invalid transaction ID format", "ETRANSID", {
        context: { parsedValue: tidRow.value }
      });
    }

    return tid;
  } catch (error) {
    throw new BackupError("Transaction ID parsing failed", "ETRANSID", {
      cause: error,
      context: { subvolPath }
    });
  }
};

const parseConfig = (): AppConfig => ({
  jsonOutput: Deno.args.includes("--json"),
  colorOutput: Deno.args.includes("--color") && Deno.stdout.isTerminal(),
  showProgress: Deno.stdout.isTerminal() && !Deno.args.includes("--no-progress"),
  destMount: path.normalize(config.destMount),
  snapDir: path.normalize(config.snapDir),
  sourceVol: path.normalize(config.sourceVol)
});

const extractUuidFromOutput = (output: Uint8Array): string | null => {
  const text = new TextDecoder().decode(output);
  return text.match(/UUID:\s+([0-9a-f-]{36})/i)?.[1] ?? null;
};

// ==================== Updated Lockfile Verification ====================
const verifyLockFile = async (): Promise<void> => {
  try {
    await Deno.lstat(config.lockFile);
  } catch (error) {
    const context: ErrorContext = {
      path: config.lockFile,
      suggestions: error instanceof Deno.errors.NotFound ? [
        "Lock file does not exist - creating new one"
      ] : error instanceof Deno.errors.PermissionDenied ? [
        "Run with appropriate permissions (try sudo)",
        `Check ownership of ${config.lockFile}`
      ] : []
    };

    const code = error instanceof Deno.errors.AlreadyExists ? "ELOCK" :
      error instanceof Deno.errors.PermissionDenied ? "EINVALID" : "ELOCK";

    throw new BackupError("Lock file verification failed", code, {
      cause: error,
      context
    });
  }
};

// ==================== Updated Mount Operations ====================
async function ensureMounted(path: string, config: AppConfig): Promise<void> {
  try {
    await verifyMountPoint(path);
    userFeedback.info(`Verified mount point: ${path}`, config);
  } catch (_error) {
    userFeedback.warning(`Mount point ${path} not active, attempting mount...`, config);
    
    try {
      await executeCommand("mount", [path], { signal: abortController.signal });
      userFeedback.success(`Successfully mounted ${path}`, config);
      
      // Verify mount after successful attempt
      await verifyMountPoint(path);
    } catch (mountError) {
      throw new BackupError(`Mount operation failed for ${path}`, "EMOUNT", {
        cause: mountError,
        context: {
          path,
          suggestions: [
            "Check /etc/fstab entries",
            "Verify filesystem integrity",
            "Ensure proper permissions"
          ]
        }
      });
    }
  }
}

// ==================== Modified Main Workflow ====================
const main = async () => {
  setup(LOG_CONFIG);
  const config = parseConfig();

  // Register signal handlers when execution starts
  Deno.addSignalListener("SIGINT", signalHandlers.SIGINT);
  Deno.addSignalListener("SIGTERM", signalHandlers.SIGTERM);
  Deno.addSignalListener("SIGHUP", signalHandlers.SIGHUP);

  try {
    await verifyLockFile();
    await withLock(async () => {
      const state = BackupState.getInstance();
      
      userFeedback.info("Starting backup process", config);
      try {
        await verifyDependencies();
        userFeedback.info("Dependencies verified", config);
        
        await ensureMounted(config.sourceVol, config);
        userFeedback.info(`Mounted source volume: ${config.sourceVol}`, config);
        
        await ensureMounted(config.destMount, config);
        userFeedback.info(`Mounted destination: ${config.destMount}`, config);

        // Create new snapshot
        userFeedback.progress("Creating new snapshot...", config);
        await createSnapshot();
        const newState = state.with({ snapshotCreated: true });

        // Determine backup type
        const parentSnap = await findParentSnapshot();
        const isFullBackup = parentSnap === null;

        // Perform backup
        if (isFullBackup) {
          userFeedback.info("Starting full backup", config);
          await performFullBackup(config.showProgress);
        } else {
          userFeedback.info(`Starting incremental backup from ${parentSnap}`, config);
          await performIncrementalBackup(parentSnap, config.showProgress);
        }

        // Update success state
        newState.with({ backupSuccessful: true });
        userFeedback.success("Backup completed successfully", config);
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : String(error);
        userFeedback.error(`Backup failed: ${errMsg}`, config);
        throw error;
      }
    });
  } catch (error) {
    await cleanup();
    const logger = getLogger();
    const formattedError = error ? 
      (error instanceof Error ? error.message : String(error)) : 
      "Unknown error occurred";
    logger.error(formattedError);
    Deno.exit(1);
  }
};

// Final verification of main error handler
main().catch(error => {
  try {
    const logger = getLogger();
    let formattedError;
    
    try {
      const formatter = new ErrorFormatter(parseConfig());
      formattedError = error 
        ? (error instanceof Error 
            ? formatter.format(error) 
            : `Non-Error: ${Deno.inspect(error)}`) 
        : "Unknown error occurred";
    } catch (formatError) {
      // Fallback if formatter fails
      formattedError = error instanceof Error 
        ? `${error.name}: ${error.message}` 
        : String(error || "Unknown error");
      
      console.error("Error while formatting error:", formatError);
    }
    
    logger.error(formattedError);
  } catch (loggerError) {
    // Last resort if logger fails
    console.error("Original error:", error);
    console.error("Logger error:", loggerError);
  } finally {
    Deno.exit(1);
  }
});