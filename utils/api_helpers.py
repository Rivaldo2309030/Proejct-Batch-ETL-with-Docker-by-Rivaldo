import requests
import logging

def fetch_api_json(url):
    """
    Realiza una solicitud GET a la URL y devuelve la respuesta JSON.
    Regresa None y loguea error si falla la solicitud.
    """
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"API call failed with status {response.status_code} for URL: {url}")
            return None
    except Exception as e:
        logging.error(f"Error fetching API data from {url}: {e}")
        return None
