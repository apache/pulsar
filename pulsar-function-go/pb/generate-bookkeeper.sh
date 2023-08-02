#!/bin/sh
if [[ `uname` == "Darwin" ]]; then
    SP=" " # Needed for portability with sed
fi
set -e

plref=3.0.0
bkref=release-4.16.1
echo pulling bookkeeper stream proto
mkdir bk

curl -sL https://github.com/apache/bookkeeper/archive/refs/tags/$bkref.tar.gz | tar xz -C bk --strip-components=6 bookkeeper-$bkref/stream/proto/src/main/proto

for file in bk/*.proto; do
    pkg_name=$(grep "^package" "$file" |sed -n 's#package \(.*\);#\1#p'|sed -rn 's#proto\.?##p' | sed 's#\.#\/#g')
    sed -r -i"${SP}"'' "s#^option java_package.*#option go_package = \"$pkg_name\";#" "$file"
done

echo generating bookkeeper libs
protoc --proto_path bk --go_out=./ --go-grpc_out=./ bk/*.proto

cat <<EOF > ./bookkeeper/doc.go
// Package bookkeeper provides the protocol buffer messages that Bookkeeper
// uses for the stream API.
//
// The protocol definition files are part of the main Bookkeper source,
// located within the Bookkeeper repository at:
// https://github.com/apache/bookkeeper/tree/master/stream/proto/src/main/proto
//
// The generated Go code was created from the source Bookkeeper files at git:
//    tag:      ${bkref}
//
// Files generated by the protoc-gen-go program should not be modified.
package bookkeeper
EOF

rm -r bk