#!/bin/bash
#
# Upgrade project's revision
# $ ./rev-upgrade.sh project rev-new
# e.g.:
# ./rev-upgrade.sh "gopkg.in/src-d/go-mysql-server.v0" "0123456789abcdef"
#
#set -x

read -d '' SCRIPT << 'EOF'
BEGIN {
	PRJ = ARGV[2]
    gsub(/^[ \t\r\n\'\"]+/, "", PRJ)
    gsub(/[ \t\r\n\'\"]+$/, "", PRJ)
	ARGC = 2

    DEPTH = 0
}

/\[\[projects\]\]/ {
	DEPTH = 1
}

/name/ {
		if (DEPTH == 1) {
			split($0, name, "=")
			prj = name[2]
            gsub(/^[ \t\r\n\'\"]+/, "", prj)
            gsub(/[ \t\r\n\'\"]+$/, "", prj)
			if (prj == PRJ) {
				DEPTH = 2
			}
		}
}

/revision/ {
		if (DEPTH == 2) {
			split($0, revision, "=")
            gsub(/^[ \t\r\n\'\"]+/, "", revision[2])
            gsub(/[ \t\r\n\'\"]+$/, "", revision[2])
            print revision[2]
			DEPTH = 0
		}
}
EOF

GOPKG="$(git rev-parse --show-toplevel)/Gopkg.lock"
PRJ="$1"
shift
REV_NEW="$1"
REV_OLD="$(awk  "$SCRIPT" "$GOPKG" "$PRJ")"

echo "Project: $PRJ"
echo "Old rev: $REV_OLD"
echo "New rev: $REV_NEW"

if [ $REV_OLD == $REV_NEW ]; then
    exit 0
fi

for file in $(git ls-files | xargs egrep -l $REV_OLD); do
    echo "# $file"
    sed -i '' "s/$REV_OLD/$REV_NEW/g" $file
done

err=$?
if [ $err -ne 0 ]; then
    exit $err
fi

dep ensure
