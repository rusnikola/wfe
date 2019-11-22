#!/bin/bash

SRCDIR=$(dirname "$BASH_SOURCE")

Rscript "$SRCDIR/genfigs_write.R"
Rscript "$SRCDIR/genfigs_write_retired.R"
Rscript "$SRCDIR/genfigs_read.R"
Rscript "$SRCDIR/genfigs_read_retired.R"
