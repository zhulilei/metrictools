# Copyright 2012, guxianje. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s
all:
	cd apps/metric_archive; go build
	cd apps/metric_notify; go build
	cd apps/metric_processor; go build
	cd apps/metric_skyline; go build
	cd apps/metric_statistic; go build
	cd apps/metric_webapi; go build

fmt:
	cd apps/metric_archive; go fmt
	cd apps/metric_notify; go fmt
	cd apps/metric_processor; go fmt
	cd apps/metric_skyline; go fmt
	cd apps/metric_statistic; go fmt
	cd apps/metric_webapi; go fmt

clean:
	cd apps/metric_archive; go clean
	cd apps/metric_notify; go clean
	cd apps/metric_processor; go clean
	cd apps/metric_skyline; go clean
	cd apps/metric_statistic; go clean
	cd apps/metric_webapi; go clean

lint:
	cd apps/metric_archive; golint *.go
	cd apps/metric_notify; golint *.go
	cd apps/metric_processor; golint *.go
	cd apps/metric_skyline; golint *.go
	cd apps/metric_statistic; golint *.go
	cd apps/metric_webapi; golint *.go
