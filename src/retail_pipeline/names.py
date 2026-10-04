"""Random but repeatable names for stores and products.

The task asks for random names. But a name must not change between runs:
"MO" should have the same store name today, tomorrow, and after the database
is rebuilt. So I seed Faker with a number made from the code. The same code
always gives the same seed, and the same seed always gives the same name.
"""

from __future__ import annotations

import hashlib

from faker import Faker

STORE_NAME_SUFFIXES: tuple[str, ...] = (
    "Market",
    "Supermarket",
    "Express",
    "Store",
    "Shop",
    "Corner",
    "Center",
)

PRODUCT_NOUNS: tuple[str, ...] = (
    "Backpack",
    "Lamp",
    "Notebook",
    "Jacket",
    "Headphones",
    "Mug",
    "Blanket",
    "Watch",
    "Sneakers",
    "Speaker",
)


def stable_seed(text: str) -> int:
    """Turn a text into a number that is always the same for the same text.

    I do not use Python's built-in hash(), because it gives a different
    number every time Python starts (for security reasons). sha256 always
    gives the same result.
    """
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


class NameGenerator:
    """Creates names like "Lake Tammy Market" (store) or "Teal Backpack" (product).

    The output depends on the Faker version. That is why Faker has a fixed
    version in requirements.txt. Names are also stored in the database and
    never changed, so even a Faker upgrade would not rename existing stores.
    """

    def __init__(self, locale: str = "en_US") -> None:
        self._faker = Faker(locale)

    def store_name(self, store_code: str) -> str:
        self._seed("store", store_code)
        return f"{self._faker.city()} {self._faker.random_element(STORE_NAME_SUFFIXES)}"

    def product_name(self, product_code: str) -> str:
        self._seed("product", product_code)
        # safe_color_name() gives simple colors like "navy" or "olive".
        color = self._faker.safe_color_name().title()
        return f"{color} {self._faker.random_element(PRODUCT_NOUNS)}"

    def _seed(self, kind: str, code: str) -> None:
        # "kind" is part of the seed, so store "A" and product "A" get different names.
        self._faker.seed_instance(stable_seed(f"{kind}:{code}"))
