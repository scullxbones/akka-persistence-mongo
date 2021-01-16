#!/usr/bin/env bash

die () {
    echo >&2 "$@"
    exit 1
}

NEXT=$1
NEXT_WO_V=$(echo $1 | sed -Ee 's/^v([.0-9]+)$/\1/')
PREVIOUS=$(git describe --abbrev=0 --tags)
PREVIOUS_WO_V=$(echo $PREVIOUS | sed -Ee 's/^v([.0-9]+)$/\1/')

if [ "$PREVIOUS" = "$NEXT" ]; then
    die "Previous and next must be different prev=$PREVIOUS next=$NEXT"
fi

echo $PREVIOUS | grep -E -q '^v[0-9]+\.[0-9]+\.[0-9]+$' || die "Previous version must follow pattern v#.#.#, was $PREVIOUS"
echo $NEXT | grep -E -q '^v[0-9]+\.[0-9]+\.[0-9]+$' || die "Next version must follow pattern v#.#.#, was $NEXT"

sed -i '' -e "s/$PREVIOUS_WO_V/$NEXT_WO_V/" README.md
sed -i '' -e "s/$PREVIOUS_WO_V/$NEXT_WO_V/" docs/akka26.md
sed -i '' -e "s/^val releaseV = \"$PREVIOUS_WO_V\"$/val releaseV = \"$NEXT_WO_V\"/" build.sbt

cat <<SECTION >target/release.md

### $NEXT_WO_V
`git log $(git describe --tags --abbrev=0)..HEAD --oneline | cut -d' ' -f 2- | sed -e 's/^/* /' `
SECTION

cp docs/changelog26.md previous.md
awk '//; /^## Changelog/{while(getline<"target/release.md"){print}}' previous.md
mv previous.md docs/changelog26.md

git add .
git commit -m 'Prepare for '$NEXT' release' -S
git tag -a $NEXT -m "$BLOCK" -s

gh release create $NEXT -F target/release.md -d -t $NEXT --target master
rm target/release.md
