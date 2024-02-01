import os
import requests
import logging
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def url_exists(url: str):
    """Check if a url exists

    Args:
        url (str): a url

    Returns:
        bool: true if the url responds with a 200 status code
    """
    response = requests.head(url)
    return response.status_code == 200


def download_url_to_file(url: str, target_dir: str, file_name: str = None):
    """Download a file from a url to a target directory

    Args:
        url (str): the url to download the file from
        target_dir (str): the directory to download the file to
        file_name (str, optional): specify the downloaded filename

    Returns:
        _type_: the path to the downloaded file
    """
    # create the target dir if it doesn't exist
    output_path = os.path.abspath(target_dir)
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    if file_name:
        output_file = os.path.join(output_path, file_name)
    else:
        url_file_name = url.split("/")[-1]
        output_file = os.path.join(output_path, url_file_name)

    if not os.path.exists(output_file):
        logger.debug(f"downloading file as {os.path.basename(output_file)}")
        r = requests.get(f"{url}", stream=True)
        with open(output_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=128):
                f.write(chunk)
    else:
        logger.debug(f"{output_file} already exists")

    return output_file


def get_links_in_table(url: str):
    index = requests.get(url)
    soup = BeautifulSoup(index.content, "html.parser")

    if len(soup.select("table")) != 1:
        raise ValueError(f"Zero or more than one table found on {url}")

    # css selector, get all anchor tags in the table that are not in a header row
    table_anchors = soup.select("table > tr:not(:has(th)) > td > a")
    return [anchor["href"] for anchor in table_anchors]
