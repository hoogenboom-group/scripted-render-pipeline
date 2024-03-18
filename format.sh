#!/bin/bash
main="origin/master"
if ! base=$(git merge-base "$main" HEAD); then
  exit 1
fi
dir="${BASH_SOURCE%/*}/scripted_render_pipeline"
mapfile -t files < <(git diff --diff-filter=d --name-only "$base" "$dir")
if [[ ${#files[@]} == 0 ]]; then
  echo "no files to format"
  exit 0
fi
isort --profile black -l 79 "${files[@]}"
black -l 79 "${files[@]}"
flake8 --extend-ignore E203 "${files[@]}"
