"""Verify release artifact checks and publication without remote writes."""

import io
import json
import tarfile
import zipfile
from pathlib import Path

import pytest

from scripts.check_release import check_release, release_version
from scripts import publish_release as publisher


@pytest.mark.parametrize("tag", ["0.0.0", "3.7.0", "10.20.30"])
def test_release_version(tag: str) -> None:
    assert release_version(tag) == tag


@pytest.mark.parametrize(
    "tag",
    [
        "invalid", "3.7", "3.07.0", "3.7.0+local", "v3.7.0",
        "3.7.0rc1", "3.7.0a1", "3.7.0b1", "3.7.0.1", "3..0",
    ],
)
def test_invalid_release_version(tag: str) -> None:
    with pytest.raises(ValueError):
        release_version(tag)


def test_artifact_versions(tmp_path: Path) -> None:
    metadata = b"Name: mcp-airflow-api\nVersion: 3.7.0\n"
    with zipfile.ZipFile(tmp_path / "package.whl", "w") as archive:
        archive.writestr("package.dist-info/METADATA", metadata)
    with tarfile.open(tmp_path / "package.tar.gz", "w:gz") as archive:
        info = tarfile.TarInfo("package/PKG-INFO")
        info.size = len(metadata)
        archive.addfile(info, io.BytesIO(metadata))
    check_release("3.7.0", tmp_path)
    with pytest.raises(ValueError, match="expected mcp-airflow-api"):
        check_release("3.7.1", tmp_path)


@pytest.mark.parametrize("previous", [None, "3.6.9"])
def test_release_notes(monkeypatch: pytest.MonkeyPatch, previous: str | None) -> None:
    calls = []

    def command(*args: str) -> str:
        calls.append(args)
        if args[1] == "rev-parse":
            return "base" if "3.6.9" in args[-1] else "current"
        if args[1] == "log":
            return "abcdef123\0Fix bug\n\nBody with ``` and $(command)\n\0"
        return ""

    monkeypatch.setattr(publisher, "command", command)
    notes = publisher.release_notes("call518/MCP-Airflow-API", "3.7.0", previous)
    assert "Fix bug\n\nBody with ``` and $(command)" in notes
    assert "````text" in notes
    assert ("Full Changelog" in notes) == bool(previous)
    assert calls[-1][-2] == ("base..current" if previous else "current")


@pytest.mark.parametrize("existing", [False, True])
def test_publish_release(monkeypatch: pytest.MonkeyPatch, existing: bool) -> None:
    created = []

    def command(*args: str) -> str:
        if args[:2] == ("git", "rev-parse"):
            return "current"
        if args[:2] == ("gh", "api"):
            if "--paginate" in args:
                releases = [{"tag_name": "3.7.0", "draft": False}] if existing else []
                return json.dumps([releases])
            return "current"
        if args[:3] == ("gh", "release", "create"):
            created.append(args)
            assert Path(args[args.index("--notes-file") + 1]).read_text() == "notes"
        return ""

    monkeypatch.setattr(publisher, "command", command)
    monkeypatch.setattr(publisher, "release_notes", lambda *args: "notes")
    publisher.publish_release("call518/MCP-Airflow-API", "3.7.0")
    assert len(created) == (0 if existing else 1)
    if created:
        assert "--prerelease" not in created[0]
