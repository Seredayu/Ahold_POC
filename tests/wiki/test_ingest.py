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


def test_ingest_file_skips_unchanged(tmp_path):
    f = tmp_path / 'skip.md'
    f.write_text('content')
    h = ingest.file_hash(f)
    manifest = {str(f): {'hash': h, 'timestamp': '2026-01-01', 'pages': []}}
    client = None  # should never be called
    # no error = skipped correctly
    ingest.ingest_file(f, manifest, client)


def test_ingest_file_calls_api_and_writes_page(tmp_path):
    from unittest.mock import MagicMock, patch

    f = tmp_path / 'new.md'
    f.write_text('# Architecture\nLayer 1 is SAP ECC.')
    manifest = {}

    page_content = '# Architecture-Overview\n\n> Last updated: 2026-05-08\n\n## Summary\nSAP ECC is Layer 1.'
    api_response = MagicMock()
    api_response.content = [MagicMock(text=json.dumps({
        'pages': [{'name': 'Architecture-Overview', 'full_markdown_content': page_content}]
    }))]

    mock_client = MagicMock()
    mock_client.messages.create.return_value = api_response

    wiki_dir = tmp_path / 'wiki'
    wiki_dir.mkdir()
    (wiki_dir / 'Architecture-Overview.md').write_text('')

    with patch.object(ingest, 'WIKI_DIR', wiki_dir), \
         patch.object(ingest, 'MANIFEST_FILE', wiki_dir / '.manifest.json'), \
         patch.object(ingest, 'ERRORS_LOG', wiki_dir / '.errors.log'):
        ingest.ingest_file(f, manifest, mock_client)

    assert (wiki_dir / 'Architecture-Overview.md').read_text() == page_content
    assert str(f) in manifest
    assert manifest[str(f)]['pages'] == ['Architecture-Overview']


def test_ingest_file_logs_error_on_extract_failure(tmp_path):
    from unittest.mock import MagicMock, patch

    f = tmp_path / 'broken.pdf'
    f.write_bytes(b'not a real pdf')
    manifest = {}
    mock_client = MagicMock()
    errors_log = tmp_path / '.errors.log'

    with patch.object(ingest, 'ERRORS_LOG', errors_log), \
         patch.object(ingest, 'WIKI_DIR', tmp_path), \
         patch.object(ingest, 'MANIFEST_FILE', tmp_path / '.manifest.json'):
        ingest.ingest_file(f, manifest, mock_client)

    assert errors_log.exists()
    assert str(f) not in manifest  # not added to manifest on error


def test_ingest_file_does_not_update_manifest_on_api_failure(tmp_path):
    from unittest.mock import MagicMock, patch

    f = tmp_path / 'doc.md'
    f.write_text('some content')
    manifest = {}

    mock_client = MagicMock()
    mock_client.messages.create.side_effect = Exception('API error')
    errors_log = tmp_path / '.errors.log'

    with patch.object(ingest, 'ERRORS_LOG', errors_log), \
         patch.object(ingest, 'WIKI_DIR', tmp_path), \
         patch.object(ingest, 'MANIFEST_FILE', tmp_path / '.manifest.json'):
        ingest.ingest_file(f, manifest, mock_client)

    assert str(f) not in manifest
