package utils

import (
	"github.com/rs/zerolog"
)

type ZerologLogger struct {
	logger zerolog.Logger
}

func NewZerologLogger(logger zerolog.Logger) Logger {
	return &ZerologLogger{logger: logger}
}

type ZerologLogEvent struct {
	event *zerolog.Event
}

func (z *ZerologLogger) Debug() LogEvent {
	return &ZerologLogEvent{event: z.logger.Debug()}
}

func (z *ZerologLogger) Info() LogEvent {
	return &ZerologLogEvent{event: z.logger.Info()}
}

func (z *ZerologLogger) Warn() LogEvent {
	return &ZerologLogEvent{event: z.logger.Warn()}
}

func (z *ZerologLogger) Error() LogEvent {
	return &ZerologLogEvent{event: z.logger.Error()}
}

func (z *ZerologLogger) Err(err error) LogEvent {
	return &ZerologLogEvent{event: z.logger.Err(err)}
}

func (e *ZerologLogEvent) Str(key, val string) LogEvent {
	e.event = e.event.Str(key, val)
	return e
}

func (e *ZerologLogEvent) Int(key string, val int) LogEvent {
	e.event = e.event.Int(key, val)
	return e
}

func (e *ZerologLogEvent) Int64(key string, val int64) LogEvent {
	e.event = e.event.Int64(key, val)
	return e
}

func (e *ZerologLogEvent) Uint32(key string, val uint32) LogEvent {
	e.event = e.event.Uint32(key, val)
	return e
}

func (e *ZerologLogEvent) Interface(key string, val interface{}) LogEvent {
	e.event = e.event.Interface(key, val)
	return e
}

func (e *ZerologLogEvent) Err(err error) LogEvent {
	e.event = e.event.Err(err)
	return e
}

func (e *ZerologLogEvent) Msg(msg string) {
	e.event.Msg(msg)
}

func (e *ZerologLogEvent) Msgf(format string, v ...interface{}) {
	e.event.Msgf(format, v...)
}

func (e *ZerologLogEvent) Strs(key string, vals []string) LogEvent {
	e.event = e.event.Strs(key, vals)
	return e
}

func (e *ZerologLogEvent) Any(key string, val interface{}) LogEvent {
	e.event = e.event.Interface(key, val)
	return e
}

func (e *ZerologLogEvent) Uint8(key string, val uint8) LogEvent {
	e.event = e.event.Uint8(key, val)
	return e
}

func (e *ZerologLogEvent) Type(key string, val interface{}) LogEvent {
	e.event = e.event.Type(key, val)
	return e
}
