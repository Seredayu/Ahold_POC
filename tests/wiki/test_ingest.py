import json
import pytest
from pathlib import Path
from unittest.mock import patch
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'wiki'))

import ingest


def test_load_manifest_missing_returns_empty(tmp_path):
    with patch.object(ingest, 'MANIFEST_FILE', tmp_path / 'missing.json'):
        assert ingest.load_manifest() == {}


def test_load_manifest_reads_existing(tmp_path):
    m = tmp_path / 'manifest.json'
    m.write_text('{"foo": {"hash": "abc"}}')
    with patch.object(ingest, 'MANIFEST_FILE', m):
        result = ingest.load_manifest()
    assert result == {'foo': {'hash': 'abc'}}


def test_save_and_reload_manifest(tmp_path):
    m = tmp_path / 'manifest.json'
    data = {'bar': {'hash': '123', 'timestamp': '2026-05-08', 'pages': ['Index']}}
    with patch.object(ingest, 'MANIFEST_FILE', m):
        ingest.save_manifest(data)
        result = ingest.load_manifest()
    assert result == data


def test_is_changed_new_file(tmp_path):
    f = tmp_path / 'new.md'
    f.write_text('hello')
    assert ingest.is_changed(f, {}) is True


def test_is_changed_same_hash(tmp_path):
    f = tmp_path / 'same.md'
    f.write_text('hello')
    h = ingest.file_hash(f)
    manifest = {str(f): {'hash': h}}
    assert ingest.is_changed(f, manifest) is False


def test_is_changed_different_hash(tmp_path):
    f = tmp_path / 'changed.md'
    f.write_text('hello')
    manifest = {str(f): {'hash': 'old_hash'}}
    assert ingest.is_changed(f, manifest) is True
