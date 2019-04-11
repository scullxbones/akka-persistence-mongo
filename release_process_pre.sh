#!/usr/bin/env bash

die () {
    echo >&2 "$@"
    exit 1
}

if [ "$GH_TOKEN" == "" ]; then
    die "Missing github oauth token"
fi

NEXT=$1
NEXT_WO_V=$(echo $1 | sed -Ee 's/^v([.0-9]+)$/\1/')
PREVIOUS=$(git describe --tags `git rev-list --tags --max-count=1`)
PREVIOUS_WO_V=$(echo $PREVIOUS | sed -Ee 's/^v([.0-9]+)$/\1/')

if [ "$PREVIOUS" = "$NEXT" ]; then
    die "Previous and next must be different prev=$PREVIOUS next=$NEXT"
fi

echo $PREVIOUS | grep -E -q '^v[0-9]+\.[0-9]+\.[0-9]+$' || die "Previous version must follow pattern v#.#.#, was $PREVIOUS"
echo $NEXT | grep -E -q '^v[0-9]+\.[0-9]+\.[0-9]+$' || die "Next version must follow pattern v#.#.#, was $NEXT"

sed -i '' -e "s/$PREVIOUS_WO_V/$NEXT_WO_V/" README.md
sed -i '' -e "s/$PREVIOUS_WO_V/$NEXT_WO_V/" docs/akka25.md
sed -i '' -e "s/^val releaseV = \"$PREVIOUS_WO_V\"$/val releaseV = \"$NEXT_WO_V\"/" build.sbt

read -r -d '' BLOCK <<SECTION
### $NEXT_WO_V
`git log $(git describe --tags --abbrev=0)..HEAD --oneline | cut -d' ' -f 2- | sed -e 's/^/* /' `

SECTION

perl -ni -e "print; print \"\n$BLOCK\n\" if $. == 1" docs/changelog25.md

git add .
git commit -m 'Prepare for '$NEXT' release' -S
git tag -a $NEXT -m "$BLOCK" -s

CR=$(printf '\r')
BLOCK_WITH_CR=$(echo -e "$BLOCK" | sed -e "s/\$/$CR/g")

cat <<API_JSON >api.json 
{
    "tag_name": "$NEXT",
    "target_commitish": "master",
    "name": "$NEXT",
    "draft": true,
    "body": "$BLOCK"
}
API_JSON

curl -v -H "Content-Type: application/json" -XPOST \
    -d @api.json \
    https://api.github.com/repos/scullxbones/akka-persistence-mongo/releases?access_token=$GH_TOKEN

rm api.json
