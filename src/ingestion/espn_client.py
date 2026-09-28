"""Bound ESPN requests without changing the process-wide requests transport."""

import requests
from espn_api.requests.espn_requests import (
    ESPNAccessDenied,
    ESPNInvalidLeague,
    ESPNUnknownError,
    EspnFantasyRequests,
)


class TimedEspnFantasyRequests(EspnFantasyRequests):
    """An ESPN client whose HTTP GETs carry an instance-specific timeout."""

    def __init__(self, *args, timeout_seconds: int, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout_seconds = timeout_seconds

    def _get(self, endpoint: str, params: dict | None, headers: dict | None):
        return requests.get(
            endpoint,
            params=params,
            headers=headers,
            cookies=self.cookies,
            timeout=self.timeout_seconds,
        )

    def checkRequestStatus(
        self, status: int, extend: str = "", params: dict = None, headers: dict = None
    ) -> dict:
        if status == 401:
            original_endpoint = self.LEAGUE_ENDPOINT
            if "/leagueHistory/" in original_endpoint:
                base = original_endpoint.split("/leagueHistory/")[0]
                alternate = (
                    f"{base}/seasons/{self.year}/segments/0/leagues/{self.league_id}"
                )
            else:
                base = original_endpoint.split("/seasons/")[0]
                alternate = (
                    f"{base}/leagueHistory/{self.league_id}?seasonId={self.year}"
                )

            response = self._get(alternate + extend, params, headers)
            if response.status_code == 200:
                self.LEAGUE_ENDPOINT = alternate
                return response.json()
            if (
                not self.cookies
                or "espn_s2" not in self.cookies
                or "SWID" not in self.cookies
            ):
                raise ESPNAccessDenied("espn_s2 and swid are required")
            raise ESPNAccessDenied(
                f"League {self.league_id} cannot be accessed with the provided credentials"
            )
        if status == 404:
            raise ESPNInvalidLeague(f"League {self.league_id} does not exist")
        if status != 200:
            raise ESPNUnknownError(f"ESPN returned an HTTP {status}")
        return None

    def league_get(self, params: dict = None, headers: dict = None, extend: str = ""):
        endpoint = self.LEAGUE_ENDPOINT + extend
        response = self._get(endpoint, params, headers)
        alternate = self.checkRequestStatus(
            response.status_code, extend, params, headers
        )
        data = alternate if alternate else response.json()
        if self.logger:
            self.logger.log_request(
                endpoint=self.LEAGUE_ENDPOINT + extend,
                params=params,
                headers=headers,
                response=data,
            )
        return data[0] if isinstance(data, list) else data

    def get(self, params: dict = None, headers: dict = None, extend: str = ""):
        endpoint = self.ENDPOINT + extend
        response = self._get(endpoint, params, headers)
        self.checkRequestStatus(response.status_code)
        data = response.json()
        if self.logger:
            self.logger.log_request(
                endpoint=endpoint, params=params, headers=headers, response=data
            )
        return data
