from datetime import datetime, timezone
from pathlib import Path

import pytest

from retail_pipeline.exceptions import SourceExtractionError
from retail_pipeline.extract import (
    KaggleDatasetDownloader,
    prune_old_snapshots,
    snapshot_name,
)


class FakeKaggleClient:
    """A fake Kaggle client for tests. It writes files instead of downloading."""

    def __init__(self, files: dict[str, str] | None = None, error: Exception | None = None):
        self.files = files if files is not None else {"data.csv": "a,b\n1,2\n"}
        self.error = error
        self.authenticated = False

    def authenticate(self) -> None:
        self.authenticated = True

    def dataset_download_files(self, dataset, path=None, force=False, quiet=True, unzip=False):
        if self.error:
            raise self.error
        for name, content in self.files.items():
            (Path(path) / name).write_text(content)


def make_downloader(client: FakeKaggleClient) -> KaggleDatasetDownloader:
    return KaggleDatasetDownloader("owner/dataset", client_factory=lambda: client)


def test_download_returns_the_csv_path(tmp_path):
    client = FakeKaggleClient()

    csv_path = make_downloader(client).download(tmp_path / "2026-10-04")

    assert client.authenticated
    assert csv_path == tmp_path / "2026-10-04" / "data.csv"
    assert csv_path.read_text() == "a,b\n1,2\n"


def test_download_replaces_previous_content_of_the_snapshot(tmp_path):
    target = tmp_path / "2026-10-04"
    target.mkdir()
    (target / "stale.csv").write_text("old")

    csv_path = make_downloader(FakeKaggleClient()).download(target)

    assert csv_path.name == "data.csv"
    assert not (target / "stale.csv").exists()


def test_client_errors_are_wrapped(tmp_path):
    client = FakeKaggleClient(error=OSError("connection reset"))

    with pytest.raises(SourceExtractionError, match="connection reset"):
        make_downloader(client).download(tmp_path / "snap")


@pytest.mark.parametrize("files", [{}, {"a.csv": "x", "b.csv": "y"}])
def test_download_requires_exactly_one_csv(tmp_path, files):
    with pytest.raises(SourceExtractionError, match="exactly one CSV"):
        make_downloader(FakeKaggleClient(files=files)).download(tmp_path / "snap")


def test_prune_keeps_the_most_recent_snapshots(tmp_path):
    for name in ["2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04"]:
        (tmp_path / name).mkdir()

    deleted = prune_old_snapshots(tmp_path, keep=2)

    assert [path.name for path in deleted] == ["2026-10-01", "2026-10-02"]
    assert sorted(path.name for path in tmp_path.iterdir()) == ["2026-10-03", "2026-10-04"]


def test_prune_on_missing_folder_does_nothing(tmp_path):
    assert prune_old_snapshots(tmp_path / "missing", keep=3) == []


def test_prune_rejects_invalid_keep(tmp_path):
    with pytest.raises(ValueError):
        prune_old_snapshots(tmp_path, keep=0)


def test_snapshot_name_uses_the_local_date():
    # 22:30 UTC on 3 October is already 4 October in Tbilisi (UTC+4).
    moment = datetime(2026, 10, 3, 22, 30, tzinfo=timezone.utc)

    assert snapshot_name(moment, "Asia/Tbilisi") == "2026-10-04"


def test_snapshot_name_needs_a_timezone():
    with pytest.raises(ValueError, match="timezone"):
        snapshot_name(datetime(2026, 10, 3, 22, 30), "Asia/Tbilisi")
