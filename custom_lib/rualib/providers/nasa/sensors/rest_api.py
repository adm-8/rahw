from rualib.providers.nasa.hooks.rest_api import NasaRestAPIHook
from airflow.utils.context import Context
from airflow.sensors.base import BaseSensorOperator
from typing import TYPE_CHECKING, Any, Sequence

import json

class NasaRestAPISensor(BaseSensorOperator):
    template_fields: Sequence[str] = (
        "s_date",
        "e_date",
    )
    ui_color = "#C5CAE9"

    def __init__(
        self,
        #*,
        s_date: str = "{{ ds }}",
        e_date: str = "{{ ds }}",
        method: str = "asteroid",
        nasa_conn_id: str = "nasa_api",
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.s_date = s_date
        self.e_date = e_date
        self.method = method
        self.nasa_conn_id = nasa_conn_id

    def poke(self, context: Context) -> bool:
        pass
        hook = NasaRestAPIHook(nasa_conn_id=self.nasa_conn_id)
        if self.method != "asteroid":
            raise ValueError('Unknown method.')
        else:
            response = hook._get_asteroids_data(self.s_date, self.e_date)
            if response.status_code != 200:
                raise ValueError(f"Error while fetching data.\nResponse code = {response.status_code}\nResponse text = {response.text}")
            else:
                resp_text = response.text
                print(f"[+] NASA Sensor response: {resp_text}")
                ec = json.loads(resp_text)['element_count']
                print(f"Element Count = {ec}")
                return bool(ec)

