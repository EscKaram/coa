#!/bin/sh
# Генерирует FastAPI-роутеры и pydantic-модели из openapi/openapi.yaml в generated/marketplace_api.
# Запускается внутри образа openapitools/openapi-generator-cli (см. Makefile и Dockerfile).
set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
GENERATOR_JAR=${GENERATOR_JAR:-/opt/openapi-generator/modules/openapi-generator-cli/target/openapi-generator-cli.jar}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

java -jar "$GENERATOR_JAR" generate \
    --input-spec "$ROOT/openapi/openapi.yaml" \
    --generator-name python-fastapi \
    --template-dir "$ROOT/openapi/templates" \
    --output "$TMP" \
    --additional-properties packageName=marketplace_api,fastapiImplementationPackage=app.impl \
    --global-property apiTests=false,modelTests=false,apiDocs=false,modelDocs=false \
    > "$TMP/generator.log" || { cat "$TMP/generator.log"; exit 1; }

# В проект попадают только роутеры, базовые классы реализаций и модели.
# Сгенерированные main.py, Dockerfile, requirements.txt и т.п. не используются.
rm -rf "$ROOT/generated"
mkdir -p "$ROOT/generated"
cp -R "$TMP/src/marketplace_api" "$ROOT/generated/marketplace_api"
rm -f "$ROOT/generated/marketplace_api/main.py"
echo "generated: $(find "$ROOT/generated" -name '*.py' | wc -l | tr -d ' ') files in generated/marketplace_api"
