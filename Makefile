# Copyright 2012, guxianje. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s
all:
	cd metricarchive; go build
	cd metricprocessor; go build
	cd metricstatistic; go build
	cd metricnotify; go build
	cd metricweb; go build

fmt:
	go fmt
	cd metricarchive; go fmt
	cd metricprocessor; go fmt
	cd metricstatistic; go fmt
	cd metricnotify; go fmt
	cd metricweb; go fmt

clean:
	cd metricarchive; go clean
	cd metricprocessor; go clean
	cd metricstatistic; go clean
	cd metricnotify; go clean
	cd metricweb; go clean
