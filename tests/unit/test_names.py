import os
import subprocess
import sys
from pathlib import Path

from retail_pipeline.names import NameGenerator, stable_seed

SRC_DIR = Path(__file__).resolve().parents[2] / "src"

# 50 states, DC, 8 territories and 3 military codes: 62 codes, like the real data.
STATE_CODES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
    "AS",
    "GU",
    "MP",
    "PR",
    "VI",
    "FM",
    "MH",
    "PW",
    "AA",
    "AE",
    "AP",
]


def test_stable_seed_is_repeatable():
    assert stable_seed("store:MO") == stable_seed("store:MO")
    assert stable_seed("store:MO") != stable_seed("store:HI")


def test_same_code_gives_same_name_in_new_generator():
    assert NameGenerator().store_name("MO") == NameGenerator().store_name("MO")
    assert NameGenerator().product_name("A") == NameGenerator().product_name("A")


def test_name_does_not_depend_on_order_of_calls():
    generator = NameGenerator()
    alone = NameGenerator().store_name("MO")

    for code in ("HI", "TX", "CA"):
        generator.store_name(code)

    assert generator.store_name("MO") == alone


def test_name_is_the_same_in_a_new_python_process():
    # Python's hash() changes in every new process. This test proves that
    # stable_seed does not have that problem.
    script = (
        "from retail_pipeline.names import NameGenerator; print(NameGenerator().store_name('MO'))"
    )
    env = {**os.environ, "PYTHONPATH": str(SRC_DIR), "PYTHONHASHSEED": "random"}

    result = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == NameGenerator().store_name("MO")


def test_all_62_store_codes_get_different_names():
    names = [NameGenerator().store_name(code) for code in STATE_CODES]

    assert len(STATE_CODES) == 62
    assert len(set(names)) == 62


def test_product_names_are_different():
    names = {NameGenerator().product_name(code) for code in "ABCD"}

    assert len(names) == 4
