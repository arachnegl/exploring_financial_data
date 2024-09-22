import datetime as dt

import requests


class FcaDataApi:
    """
    docs:
    https://www.fca.org.uk/publication/systems-information/fca-fitrs-instructions_0.pdf
    """

    _firds_or_fitrs_to_file_type = {
        "firds": "FULINS",
        "fitrs": "Full",
    }

    @classmethod
    def get_latest_urls(cls, firds_or_fitrs: str) -> list[str]:
        # FIRDS Full files are published weekly on Saturday
        now = dt.datetime.now()
        dt_max = now.date()
        dt_min = dt_max - dt.timedelta(days=7)
        return cls.get_urls(firds_or_fitrs, dt_min, dt_max)

    @classmethod
    def get_urls(
        cls, firds_or_fitrs: str, dt_min: dt.date, dt_max: dt.date
    ) -> list[str]:

        if firds_or_fitrs not in cls._firds_or_fitrs_to_file_type:
            raise ValueError(
                f"{firds_or_fitrs} must be one of {cls._firds_or_fitrs_to_file_type.keys()}"
            )
        file_type = cls._firds_or_fitrs_to_file_type[firds_or_fitrs]

        response = requests.get(
            f"https://api.data.fca.org.uk/fca_data_{firds_or_fitrs}_files",
            params={
                # q is an Elastic Search query string
                "q": f"((file_type:{file_type}) AND (publication_date:[{dt_min} TO {dt_max}]))",
                "from": 0,
                "size": 100,
                "pretty": "true",
            },
        )

        if response.status_code != 200:
            response.raise_for_status()

        return cls._transform_payload(response.json())

    @classmethod
    def _transform_payload(cls, payload: dict) -> list[str]:
        return [file["_source"]["download_link"] for file in payload["hits"]["hits"]]
