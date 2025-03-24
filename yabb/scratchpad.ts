// New logging module using @std/log
import { setup, getLogger, ConsoleHandler } from "jsr:@std/log";
import type { LogRecord, LevelName, LogConfig } from "jsr:@std/log";

// Replicate Color functions from yabb.ts since we can't modify original
const Color = {
  red: (text: string) => `\x1b[31m${text}\x1b[0m`,
  green: (text: string) => `\x1b[32m${text}\x1b[0m`,
  yellow: (text: string) => `\x1b[33m${text}\x1b[0m`,
  cyan: (text: string) => `\x1b[36m${text}\x1b[0m`,
  gray: (text: string) => `\x1b[90m${text}\x1b[0m`,
  bold: (text: string) => `\x1b[1m${text}\x1b[22m`,
};

// Replicate necessary interfaces
interface AppConfig {
  readonly jsonOutput: boolean;
  readonly colorOutput: boolean;
  readonly showProgress: boolean;
}

// Type-safe log level extensions
type ExtendedLevelName = LevelName | "SUCCESS" | "PROGRESS";

const LOG_CONFIG: LogConfig = {
  handlers: {
    console: new ConsoleHandler("DEBUG", {
      formatter: (logRecord: LogRecord) => {
        const message = customFormatter(logRecord);
        return `${logRecord.datetime.toISOString()} ${message}`;
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

function customFormatter(logRecord: LogRecord): string {
  const { levelName, args } = logRecord;
  const [message, config] = args as [string, AppConfig];
  const rest = args.slice(2);

  const colorMap: Record<string, (text: string) => string> = {
    DEBUG: Color.gray,
    INFO: Color.cyan,
    WARN: Color.yellow,
    ERROR: Color.red,
    CRITICAL: Color.red,
    SUCCESS: Color.green,
    PROGRESS: Color.gray,
  };

  const symbolMap: Record<string, string> = {
    INFO: "ℹ️",
    WARN: "⚠️",
    SUCCESS: "✅",
    PROGRESS: "⌛",
    ERROR: "❌",
    CRITICAL: "💥",
  };

  const symbol = symbolMap[levelName] || "";
  const coloredMessage = config.colorOutput && colorMap[levelName]
    ? colorMap[levelName](`${symbol} ${message}`)
    : `${symbol} ${message}`;

  return coloredMessage + (rest.length > 0 ? ` ${rest.join(" ")}` : "");
}

// Recreate the userFeedback interface using std/log
export const userFeedback = {
  info: (message: string, config: AppConfig) => {
    const logger = getLogger();
    if (!config.jsonOutput) logger.info(message, config);
  },
  
  warning: (message: string, config: AppConfig) => {
    const logger = getLogger();
    if (!config.jsonOutput) logger.warn(message, config);
  },

  success: (message: string, config: AppConfig) => {
    const logger = getLogger();
    if (!config.jsonOutput) logger.info(message, config, "SUCCESS" as LevelName);
  },

  progress: (message: string, config: AppConfig) => {
    const logger = getLogger();
    if (config.showProgress && !config.jsonOutput) {
      logger.info(message, config, "PROGRESS" as LevelName);
    }
  }
};

// Initialize logger with configuration
setup(LOG_CONFIG); 