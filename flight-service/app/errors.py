"""Бизнес-ошибки Flight Service и их представление в статусе gRPC.

Код статуса — стандартный gRPC (NOT_FOUND, RESOURCE_EXHAUSTED, ...). Машиночитаемая причина
передаётся в google.rpc.ErrorInfo внутри google.rpc.Status (trailer grpc-status-details-bin),
как описано в proto/flight/v1/flight_service.proto.
"""

from typing import Any

import grpc
from google.protobuf import any_pb2
from google.rpc import error_details_pb2, status_pb2

ERROR_DOMAIN = "flight.v1"
STATUS_DETAILS_KEY = "grpc-status-details-bin"


class FlightError(Exception):
    def __init__(self, code: grpc.StatusCode, reason: str, message: str, **metadata: Any) -> None:
        super().__init__(message)
        self.code = code
        self.reason = reason
        self.message = message
        self.metadata = {k: str(v) for k, v in metadata.items()}

    def trailing_metadata(self) -> tuple[tuple[str, bytes], ...]:
        info = error_details_pb2.ErrorInfo(reason=self.reason, domain=ERROR_DOMAIN, metadata=self.metadata)
        detail = any_pb2.Any()
        detail.Pack(info)
        status = status_pb2.Status(code=self.code.value[0], message=self.message, details=[detail])
        return ((STATUS_DETAILS_KEY, status.SerializeToString()),)


def invalid_argument(message: str, **metadata: Any) -> FlightError:
    return FlightError(grpc.StatusCode.INVALID_ARGUMENT, "INVALID_ARGUMENT", message, **metadata)


def flight_not_found(flight_id: Any) -> FlightError:
    return FlightError(grpc.StatusCode.NOT_FOUND, "FLIGHT_NOT_FOUND", f"flight {flight_id} not found", flight_id=flight_id)
