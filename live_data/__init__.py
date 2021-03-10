from d3a_api_client.utils import domain_name_from_env, retrieve_jwt_key_from_server, \
    logging_decorator, RestCommunicationMixin


class LiveDataRest(RestCommunicationMixin):
    def __init__(self):
        self.domain_name = domain_name_from_env
        self.jwt_token = retrieve_jwt_key_from_server(self.domain_name)
        self._create_jwt_refresh_timer(self.domain_name)

    @logging_decorator('set_energy_forecast')
    def set_energy_forecast(self, *args, **kwargs):
        print(f"set_energy_forecast")
        self.simulation_id = kwargs['simulation_id']
        self.device_id = kwargs['area_uuid']
        transaction_id, posted = self._post_request('set_energy_forecast',
                                                    {"energy_forecast": kwargs['energy_wh']})
