from fca import FcaDataApi
from transform import download_xml_to_pq
from utils import run_concurrent


def etl(firds_or_fitrs):
    # TODO add timeit
    # TODO add memory profiling

    urls = FcaDataApi.get_latest_urls(firds_or_fitrs)
    print(urls)
    pq_files = run_concurrent(
        download_xml_to_pq,
        [(url, (firds_or_fitrs, url), {}) for url in urls],
    )
    print(pq_files)
