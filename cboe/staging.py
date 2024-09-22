import subprocess
from pathlib import Path

dst_dir = Path(".src/apa")
dst_dir.mkdir(parents=True, exist_ok=True)


def download_apa_files(day: date) -> list[Path]:

    url = f"https://cdn.cboe.com/data/europe/equities/trade_data/0a3ca04650/hour/rts13_public_trade_data_apa_{day}_{hour:02}.csv"
    cmd = "curl -O {url}"
    subprocess.run()
