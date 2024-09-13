#!/bin/bash

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <filename> <current_version> <new_version>"
  exit 1
fi

FILENAME="$1"
CURRENT_VERSION="$2"
NEXT_VERSION="$3"

echo "$FILENAME"
echo "$CURRENT_VERSION"
echo "$NEXT_VERSION"

if [ ! -f "$FILENAME" ]; then
  echo "Error: File '$FILENAME' not found!"
  exit 1
fi

while IFS= read -r LINE; do
  if [[ "$LINE" == *"$CURRENT_VERSION"* ]]; then
    UPDATED_LINE="${LINE/$CURRENT_VERSION/$NEXT_VERSION}"
    echo "$UPDATED_LINE"
  else
    echo "$LINE"
  fi
done < "$FILENAME" > "$FILENAME.new"

mv "$FILENAME.new" "$FILENAME"
echo "License file '$FILENAME' updated successfully."