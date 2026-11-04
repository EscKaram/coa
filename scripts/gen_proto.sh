#!/bin/sh
# Генерация Python-кода (сообщения, gRPC-стабы, .pyi) из proto/ в каталог $1 (по умолчанию generated/).
# Нужны пакеты grpcio-tools и googleapis-common-protos (google/type/*.proto, google/rpc/*.proto).
# Используется на этапе codegen в Dockerfile обоих сервисов и в `make proto`.
set -eu

OUT="${1:-generated}"
mkdir -p "$OUT"

# Каталог site-packages, в котором лежат .proto из googleapis-common-protos.
GOOGLEAPIS_INCLUDE=$(python -c 'import os, google.type.money_pb2 as m; print(os.path.dirname(os.path.dirname(os.path.dirname(m.__file__))))')

python -m grpc_tools.protoc \
    -I proto \
    -I "$GOOGLEAPIS_INCLUDE" \
    --python_out="$OUT" \
    --pyi_out="$OUT" \
    --grpc_python_out="$OUT" \
    $(find proto -name '*.proto')

echo "generated into $OUT:"
find "$OUT" -name '*.py*' | sort
