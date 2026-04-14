#!/usr/bin/env sh

set -eu

PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

cd "$PROJECT_ROOT"

if [ ! -f ".env" ]; then
  echo "Aviso: arquivo .env nao encontrado na raiz do projeto." >&2
fi

if [ -x ".conda/python.exe" ]; then
  PYTHON_CMD=".conda/python.exe"
elif [ -x ".conda/bin/python" ]; then
  PYTHON_CMD=".conda/bin/python"
else
  PYTHON_CMD="python"
fi

echo "Executando pipeline em $PROJECT_ROOT"
"$PYTHON_CMD" -m pipeline.run
