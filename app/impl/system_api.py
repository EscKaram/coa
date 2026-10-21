from marketplace_api.apis.system_api_base import BaseSystemApi
from marketplace_api.models.health_response import HealthResponse


class SystemApi(BaseSystemApi):
    async def health(self) -> HealthResponse:
        return HealthResponse(status="ok")
