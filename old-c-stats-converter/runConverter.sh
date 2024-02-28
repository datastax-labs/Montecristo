#!/bin/bash
./gradlew build install -x test
./build/install/old-c-stats-converter/bin/old-c-stats-converter $@ || exit 1