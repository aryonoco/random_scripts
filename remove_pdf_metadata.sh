#!/bin/sh
# Portability:  Linux, *BSD, MacOS, Illumos (mktemp -d)
# Dependencies: Tcl (>=8.5), exiftool, mutool, qpdf
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


workdir=$(mktemp -d)
workfile=$workdir/work.pdf
trap 'exit 1' HUP INT QUIT ${ZSH_VERSION-ABRT} TERM
trap 'rm -r -- "$workdir"' EXIT

# Tcl >=8.5 script to remove the Document Information Dictionary
# Maybe parse the trailer to find the /Info ref and remove this object instead?
cat <<'EOF' >"$workdir"/clean.tcl
package require Tcl 8.5-
namespace path {::tcl::mathop ::tcl::mathfunc}

chan configure stdin -translation binary
chan configure stdout -translation binary
set data [read stdin]

# Per ISO 32000, 4.18, EOL is CR, LF or CRLF
set obj_re {\sobj(?:\r\n?|\n).*?(?:\r\n?|\n)endobj(?:\r\n?|\n)}
set docinfo_key_re {/(?:Title|Subject|Keywords|Author|CreationDate|ModDate|Creator|Producer)[[:space:](]}

set prev_end -1
foreach match [regexp -indices -all -inline $obj_re $data] {
	lassign $match start end
	puts -nonewline [string range $data [+ $prev_end 1] [- $start 1]]
	set match_data [string range $data $start $end]
	# Try to detect Document Information Dictionary by the presence of key and the absence of
	# (potentially misleading) stream
	if {[string first "endstream" $match_data] == -1 && [regexp $docinfo_key_re $match_data]} {
		set obj_header [lindex [regexp -inline {^\sobj(?:\r\n?|\n)} $match_data] 0]
		set obj_footer [lindex [regexp -inline {(?:\r\n?|\n)endobj(?:\r\n?|\n)$} $match_data] 0]
		set part1 "$obj_header<<\n"
		set part2 "/Producer (anonymize_pdf.sh)\n>>$obj_footer"
		# Needs padding to avoid breaking startxref
		set padlen [- [string length $match_data] [string length $part1] [string length $part2]]
		set pad "[string repeat " " [- $padlen 1]]\n"
		puts -nonewline "$part1$pad$part2"
	} else {
		puts -nonewline $match_data
	}
	set prev_end $end
}
puts -nonewline [string range $data [+ $prev_end 1] [string length $data]]
EOF

tclsh "$workdir"/clean.tcl <"$1" >"$workfile"
exiftool -q -q -all:all= "$workfile"

# mutool: garbage collection and stream sanitization
# qpdf: linearization to permanently remove metadata (mutool dropped -l support in 1.26)
mutool clean -gggg -c -s "$workfile" "$workfile.clean"
qpdf --linearize "$workfile.clean" "$2"