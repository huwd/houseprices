"""Main pipeline: download → join → aggregate."""


def normalise_address(saon: str, paon: str, street: str) -> str:
    raise NotImplementedError


def aggregate(rows: list[dict]) -> dict:  # type: ignore[type-arg]
    raise NotImplementedError
