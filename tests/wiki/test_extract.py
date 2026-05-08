import pytest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'wiki'))

from extract import extract_text, UnsupportedFileType

FIXTURES = Path(__file__).parent / 'fixtures'


def test_extract_markdown():
    text = extract_text(FIXTURES / 'sample.md')
    assert 'Hello world from markdown' in text


def test_extract_txt():
    text = extract_text(FIXTURES / 'sample.txt')
    assert 'Hello world from text' in text


def test_unsupported_type_raises(tmp_path):
    fake = tmp_path / 'sample.xyz'
    fake.write_text('data')
    with pytest.raises(UnsupportedFileType):
        extract_text(fake)
