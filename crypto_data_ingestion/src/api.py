import os

import requests
import yaml

# Determine the absolute path to the config file
config_path = os.path.join(os.getcwd(), 'config.yaml')

# Load the configuration file
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

# API INTERFACE


class CoinGeckoAPIClient:
    """
    An interface to interact with the CoinGecko data API.

    This class contains methods to make HTTP requests using the 'Requests' library through the 'GET' action on the CoinGecko API.

    Attributes:
        base_url (str): The base URL of the CoinGecko API.
        headers (dict): The HTTP request headers containing information such as authentication and content type.
    """

    def __init__(self):
        self.base_url: str = config['coingecko']['api_url']
        if not self.base_url:
            raise ValueError('Coingecko API URL not set in config.yaml')
        self.headers: dict = {
            'Content-Type': 'application/json',
        }

    def get_data(self, endpoint: str, params: dict = None) -> dict:
        """
        Sends a GET request to the API endpoint.

        Parameters:
            endpoint (str): The API endpoint you want to query.
            params (dict, optional): The query parameters you want to execute. Defaults to None.

        Returns:
            dict: The JSON response from the API.

        Raises:
            requests.exceptions.RequestException: If there is an issue with the HTTP request.
        """
        url = f'{self.base_url}/{endpoint}'
        try:
            response = requests.get(url, headers=self.headers, params=params)
            return self._handle_response(response)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

    def _handle_response(self, response: requests.Response) -> dict:
        """
        Handles the HTTP response from a request to the CoinGecko API.

        This method checks the status code of the response. If the status code is 200 (OK),
        it returns the JSON content of the response. Otherwise, it raises an HTTPError
        based on the status code.

        Args:
            response (requests.Response): The HTTP response object returned by the API request.

        Returns:
            dict: The JSON content of the response if the status code is 200.

        Raises:
            requests.exceptions.HTTPError: If the response has a status code other than 200.
        """
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
