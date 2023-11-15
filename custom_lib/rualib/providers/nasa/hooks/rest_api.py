from airflow.hooks.base import BaseHook
import requests


class NasaRestAPIHook(BaseHook):
    def __init__(
        self,
        nasa_conn_id: str = "nasa_api"
    ) -> None:
        super().__init__()
        self.conn = self.get_connection(nasa_conn_id)
        self.base_url = "https://api.nasa.gov"

    def _get_data_from_api(self, method_path: str, params: str):
        request_url = f"{self.base_url}/{method_path}?{params}&api_key={self.conn.password}"
        print(f"[-] [-] Fetching data from {request_url}")
        return requests.get(request_url)

    def _get_asteroids_data(self, start_date: str, end_date: str):
        return self._get_data_from_api("neo/rest/v1/feed", f"start_date={start_date}&end_date={end_date}")
