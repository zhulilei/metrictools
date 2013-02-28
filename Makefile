# Copyright 2012, guxianje. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s
all:
	cd metric_processor; go build
	cd metric_statistic; go build
	cd metric_notify; go build
	cd metric_web; go build

fmt:
	go fmt
	cd metric_processor; go fmt
	cd metric_statistic; go fmt
	cd metric_notify; go fmt
	cd metric_web; go fmt

clean:
	cd metric_processor; go clean
	cd metric_statistic; go clean
	cd metric_notify; go clean
	cd metric_web; go clean
