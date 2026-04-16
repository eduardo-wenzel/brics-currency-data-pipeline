#!/usr/bin/env sh

set -eu

ACTION="${1:-test}"
PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
DBT_DIR="$PROJECT_ROOT/dbt"

case "$ACTION" in
  test|debug|parse|deps)
    ;;
  *)
    echo "Uso: ./scripts/dbt.sh {test|debug|parse|deps}" >&2
    exit 1
    ;;
esac

if [ ! -f "$ENV_FILE" ]; then
  echo "Arquivo .env nao encontrado em $ENV_FILE. Crie-o a partir de .env.example antes de executar o dbt." >&2
  exit 1
fi

set -a
. "$ENV_FILE"
set +a

for required_var in PG_HOST PG_DATABASE PG_USER PG_PASSWORD PG_PORT; do
  eval "value=\${$required_var:-}"
  if [ -z "$value" ]; then
    echo "Variavel ausente para o dbt: $required_var. Preencha o .env e tente novamente." >&2
    exit 1
  fi
done

if command -v dbt >/dev/null 2>&1; then
  DBT_CMD="dbt"
elif [ -x "$PROJECT_ROOT/.conda/bin/dbt" ]; then
  DBT_CMD="$PROJECT_ROOT/.conda/bin/dbt"
else
  echo "Executavel do dbt nao encontrado. Ative um ambiente com dbt instalado ou instale as dependencias de dev." >&2
  exit 1
fi

"$DBT_CMD" "$ACTION" --project-dir "$DBT_DIR" --profiles-dir "$DBT_DIR"
