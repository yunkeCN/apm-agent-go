package apmgormv2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go.elastic.co/apm"
	"go.elastic.co/apm/module/apmsql"
	"gorm.io/gorm"
)

const (
	apmContextKey  = "elasticapm:context"
	callbackPrefix = "elasticapm"
)

// WithContext returns a copy of db with ctx recorded for use by
// the callbacks registered via Initialize.
func WithContext(ctx context.Context, db *gorm.DB) *gorm.DB {
	return db.Set(apmContextKey, ctx)
}

func scopeContext(scope *gorm.DB) (context.Context, bool) {
	value, ok := scope.Get(apmContextKey)
	if !ok {
		return nil, false
	}
	ctx, _ := value.(context.Context)
	return ctx, ctx != nil
}

type Config struct {
	DSN string // use DBName as metrics label
}

type ElasticAPM struct {
	*gorm.DB
	*Config
}

func NewElasticAPM(config *Config) *ElasticAPM {
	return &ElasticAPM{Config: config}
}

func (e *ElasticAPM) Name() string {
	return "gorm:elasticapm"
}

func (e *ElasticAPM) Initialize(db *gorm.DB) error { //can be called repeatedly
	e.DB = db
	driverName := e.Dialector.Name()
	switch driverName {
	case "postgres":
		driverName = "postgresql"
	}
	dsnInfo := apmsql.DriverDSNParser(driverName)(e.DSN)

	var err error
	spanTypePrefix := fmt.Sprintf("db.%s.", driverName)
	querySpanType := spanTypePrefix + "query"
	execSpanType := spanTypePrefix + "exec"

	createName := "gorm:create"
	err = e.Callback().Create().Before(createName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, createName),
		newBeforeCallback(execSpanType),
	)
	if err != nil {
		return err
	}
	err = e.Callback().Create().After(createName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, createName),
		newAfterCallback(dsnInfo),
	)
	if err != nil {
		return err
	}

	deleteName := "gorm:delete"
	err = e.Callback().Delete().Before(deleteName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, deleteName),
		newBeforeCallback(execSpanType),
	)
	if err != nil {
		return err
	}
	err = e.Callback().Delete().After(deleteName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, deleteName),
		newAfterCallback(dsnInfo),
	)
	if err != nil {
		return err
	}

	queryName := "gorm:query"
	err = e.Callback().Query().Before(queryName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, queryName),
		newBeforeCallback(querySpanType),
	)
	if err != nil {
		return err
	}
	err = e.Callback().Query().After(queryName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, queryName),
		newAfterCallback(dsnInfo),
	)
	if err != nil {
		return err
	}

	updateName := "gorm:update"
	err = e.Callback().Update().Before(updateName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, updateName),
		newBeforeCallback(execSpanType),
	)
	if err != nil {
		return err
	}
	err = e.Callback().Update().After(updateName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, updateName),
		newAfterCallback(dsnInfo),
	)
	if err != nil {
		return err
	}

	rowName := "gorm:row"
	err = e.Callback().Row().Before(rowName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, rowName),
		newBeforeCallback(querySpanType),
	)
	if err != nil {
		return err
	}
	err = e.Callback().Row().After(rowName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, rowName),
		newAfterCallback(dsnInfo),
	)
	if err != nil {
		return err
	}

	rawName := "gorm:raw"
	err = e.Callback().Raw().Before(rawName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, rawName),
		newBeforeCallback(querySpanType),
	)
	if err != nil {
		return err
	}
	err = e.Callback().Raw().After(rawName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, rawName),
		newAfterCallback(dsnInfo),
	)
	if err != nil {
		return err
	}
	return nil
}

func newBeforeCallback(spanType string) func(*gorm.DB) {
	return func(scope *gorm.DB) {
		ctx, ok := scopeContext(scope)
		if !ok {
			return
		}
		span, ctx := apm.StartSpan(ctx, "", spanType)
		if span.Dropped() {
			span.End()
			ctx = nil
		}
		scope.Set(apmContextKey, ctx)
	}
}

func newAfterCallback(dsnInfo apmsql.DSNInfo) func(*gorm.DB) {
	return func(scope *gorm.DB) {
		ctx, ok := scopeContext(scope)
		if !ok {
			return
		}
		span := apm.SpanFromContext(ctx)
		if span == nil {
			return
		}
		span.Name = apmsql.QuerySignature(scope.Statement.SQL.String())
		span.Context.SetDestinationAddress(dsnInfo.Address, dsnInfo.Port)
		span.Context.SetDatabase(apm.DatabaseSpanContext{
			Instance:  dsnInfo.Database,
			Statement: scope.Statement.SQL.String(),
			Type:      "sql",
			User:      dsnInfo.User,
		})
		defer span.End()

		// Capture errors, except for "record not found", which may be expected.
		err := scope.Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) && err != sql.ErrNoRows {
			if e := apm.CaptureError(ctx, err); e != nil {
				e.Send()
			}
		}
	}
}
