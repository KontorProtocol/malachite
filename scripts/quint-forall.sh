#!/bin/bash
#
# Run `quint` with the provided command on all provided files.
# Filenames are read from the standard input (stdin).
#

UNDERLINE=$(tput smul)
RESET=$(tput sgr0)

if [ "$#" -eq 0 ] ; then
	echo "${UNDERLINE}Usage:${RESET} $0 <command>"
	exit 1
fi

# [INFO] message in blue
BLUE=$(tput setaf 4)
info() {
	echo "${BLUE}[INFO] $*${RESET}"
} 

# [ERROR] message in red
RED=$(tput setaf 1)
error() {
	echo "${RED}[ERROR] $*${RESET} "
}

FAILED=0
FAILED_FILES=()
RAN=0

# Read input files, one per line
while IFS="" read -r file; do
	info "Running: quint $* ${UNDERLINE}$file"
	if ! time npx --yes @informalsystems/quint@0.27.0 "$@" "$file" < /dev/null; then
		FAILED_FILES+=("$file")
		FAILED=$((FAILED + 1))
	fi
	RAN=$((RAN + 1))
	echo ""
done

info "Ran $RAN specification files"
if [ "$RAN" -eq 0 ]; then
	error "No specification files were provided"
	exit 1
fi

if [ "$FAILED" -gt 0 ]; then
	error "Failed on $FAILED files:"
	for file in "${FAILED_FILES[@]}"; do
		error " - ${UNDERLINE}$file"
	done
fi

exit $FAILED
