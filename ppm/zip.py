import gzip
import shutil
import logging

logger = logging.getLogger(__name__)


def ungz(gz: str):
    source = gz
    target = gz.rstrip(".gz")  # remove the '.gz' from the end of the filename

    # Unzip the .csv.gz file
    with gzip.open(source, "rb") as f_in:
        with open(target, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    logger.debug(f"ungzipped {source} to {target}")

    return target
