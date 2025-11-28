#!/usr/bin/env sh
# Dependencies: mutool, qpdf, exiftool
set -eu

echo() { printf '%s\n' "$*"; }
die() { echo "$@" >&2; exit 1; }
has_cmd() { command -v "$1" >/dev/null; }

usage()
{
	_name=$(basename "$0")
	cat <<EOF | { [ $1 -eq 0 ] && cat || cat >&2; }
NAME
    $_name - Anonymize PDFs

SYNOPSIS
    $_name [-h] PDF_IN PDF_OUT

DESCRIPTION
    Strip the Document Information Dictionary (cf [1]) and XMP/EXIF ([2]) data
    from PDF_IN and write the result to PDF_OUT.

    [1] https://www.oreilly.com/library/view/pdf-explained/9781449321581/ch04.html#I_sect14_d1e2745
    [2] https://exiftool.org/TagNames/PDF.html

OPTIONS
    -h
        Print this notice to stdout and exit.

EOF
	exit $1
}

while getopts "h" OPT
do
	case "$OPT" in
		h)  usage 0;;
		\?) usage 1;;
	esac
done
shift $((OPTIND - 1))

[ $# -ne 2 ] && usage 1

# Check dependencies
for cmd in mutool qpdf exiftool; do
	has_cmd "$cmd" || die "Required command not found: $cmd"
done

# Validate input file
[ -f "$1" ] || die "Input file not found: $1"
[ -r "$1" ] || die "Input file not readable: $1"

# Check PDF magic bytes (%PDF-)
head -c 5 "$1" | grep -q '%PDF-' || die "Input file is not a PDF: $1"

# Check output directory is writable
outdir=$(dirname "$2")
[ -d "$outdir" ] || die "Output directory does not exist: $outdir"
[ -w "$outdir" ] || die "Output directory not writable: $outdir"

workdir=$(mktemp -d) || die "Failed to create temp directory"
workfile=$workdir/work.pdf
trap 'exit 1' HUP INT QUIT ${ZSH_VERSION-ABRT} TERM
trap 'rm -r -- "$workdir"' EXIT

# Step 1: mutool - garbage collection (-gggg), clean content streams (-c), sanitize (-s)
mutool clean -gggg -c -s "$1" "$workfile" \
	|| die "mutool failed to process input file"

# Step 2: qpdf - remove XMP metadata stream and Document Info Dictionary
qpdf --linearize --remove-metadata --remove-info "$workfile" "$workfile.clean" \
	|| die "qpdf failed to remove metadata"

# Step 3: exiftool - blank remaining dates (qpdf preserves ModifyDate by design)
exiftool -q -overwrite_original -PDF:ModifyDate= -PDF:CreateDate= "$workfile.clean" \
	|| die "exiftool failed to remove dates"

# Step 4: qpdf - re-linearize to permanently remove date metadata
qpdf --linearize "$workfile.clean" "$2" \
	|| die "qpdf failed to write output file"