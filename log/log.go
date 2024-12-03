package log

import (
	"context"
	"log/slog"
)

// Err returns [slog.Attr] for error value.
func Err(err error) slog.Attr {
	if err == nil {
		return slog.Any("err", nil)
	}
	return slog.Any("err", err.Error())
}

// Debug calls [slog.DebugContext].
func Debug(ctx context.Context, msg string, args ...any) {
	slog.DebugContext(ctx, msg, args...)
}

// Info calls [slog.InfoContext].
func Info(ctx context.Context, msg string, args ...any) {
	slog.InfoContext(ctx, msg, args...)
}

// Warn calls [slog.WarnContext].
func Warn(ctx context.Context, msg string, args ...any) {
	slog.WarnContext(ctx, msg, args...)
}

// Error calls [slog.ErrorContext].
func Error(ctx context.Context, msg string, args ...any) {
	slog.ErrorContext(ctx, msg, args...)
}
