import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Union

import anthropic

WIKI_DIR = Path(__file__).parent
RESEARCH_DIR = WIKI_DIR.parent / 'research'
MANIFEST_FILE = WIKI_DIR / '.manifest.json'
ERRORS_LOG = WIKI_DIR / '.errors.log'

WIKI_PAGES = [
    'Architecture-Overview', 'Data-Integration', 'ML-Models',
    'Replenishment-Engine', 'SAP-Integration', 'Frontend', 'Infrastructure',
]

SUPPORTED_EXTENSIONS = {'.md', '.txt', '.pdf', '.docx', '.pptx', '.xlsx'}
IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg'}
ALL_SUPPORTED_EXTENSIONS = SUPPORTED_EXTENSIONS | IMAGE_EXTENSIONS


def file_hash(filepath: Path) -> str:
    return hashlib.sha256(Path(filepath).read_bytes()).hexdigest()


def load_manifest() -> dict:
    if MANIFEST_FILE.exists():
        return json.loads(MANIFEST_FILE.read_text(encoding='utf-8'))
    return {}


def save_manifest(manifest: dict) -> None:
    MANIFEST_FILE.write_text(json.dumps(manifest, indent=2), encoding='utf-8')


def is_changed(filepath: Path, manifest: dict) -> bool:
    key = str(filepath)
    if key not in manifest:
        return True
    return manifest[key]['hash'] != file_hash(filepath)


def load_existing_pages() -> dict:
    pages = {}
    for name in WIKI_PAGES:
        page_file = WIKI_DIR / f'{name}.md'
        if page_file.exists():
            pages[name] = page_file.read_text(encoding='utf-8')
    return pages


def _log_error(filepath: Path, error: Union[Exception, str]) -> None:
    with open(ERRORS_LOG, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().isoformat()} {filepath}: {error}\n")


def ingest_file(filepath: Path, manifest: dict, client: anthropic.Anthropic) -> None:
    filepath = Path(filepath)
    if not is_changed(filepath, manifest):
        return
    suffix = filepath.suffix.lower()

    if suffix in IMAGE_EXTENSIONS:
        _ingest_image(filepath, manifest, client)
        return

    from extract import extract_text, UnsupportedFileType
    try:
        text = extract_text(filepath)
    except UnsupportedFileType as e:
        _log_error(filepath, e)
        return
    except Exception as e:
        _log_error(filepath, e)
        return

    _call_api_and_write(filepath, text, manifest, client)


def _ingest_image(filepath: Path, manifest: dict, client: anthropic.Anthropic) -> None:
    import base64
    import mimetypes
    mime = mimetypes.guess_type(str(filepath))[0] or 'image/png'
    data = base64.standard_b64encode(filepath.read_bytes()).decode('utf-8')
    existing_pages = load_existing_pages()
    existing_context = _format_existing_pages(existing_pages)

    try:
        response = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=8096,
            system=_system_prompt(),
            messages=[{
                'role': 'user',
                'content': [
                    {
                        'type': 'image',
                        'source': {'type': 'base64', 'media_type': mime, 'data': data},
                    },
                    {
                        'type': 'text',
                        'text': f'Source image: {filepath.name}\n\nCurrent wiki pages:\n\n{existing_context}',
                    },
                ],
            }],
        )
    except Exception as e:
        _log_error(filepath, e)
        return  # don't update manifest — retry on next run

    _process_response(filepath, response, manifest)


def _call_api_and_write(
    filepath: Path, text: str, manifest: dict, client: anthropic.Anthropic
) -> None:
    existing_pages = load_existing_pages()
    existing_context = _format_existing_pages(existing_pages)

    try:
        response = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=8096,
            system=_system_prompt(),
            messages=[{
                'role': 'user',
                'content': (
                    f'Source document ({filepath.name}):\n\n{text}'
                    f'\n\nCurrent wiki pages:\n\n{existing_context}'
                ),
            }],
        )
    except Exception as e:
        _log_error(filepath, e)
        return  # don't update manifest — retry on next run

    _process_response(filepath, response, manifest)


def _process_response(
    filepath: Path, response, manifest: dict
) -> None:
    try:
        result = json.loads(response.content[0].text)
    except (json.JSONDecodeError, IndexError) as e:
        _log_error(filepath, f"Bad API response: {e}")
        return

    updated_pages = []
    for page in result.get('pages', []):
        name = page.get('name', '').strip()
        content = page.get('full_markdown_content', '')
        if name and content:
            (WIKI_DIR / f'{name}.md').write_text(content, encoding='utf-8')
            updated_pages.append(name)

    manifest[str(filepath)] = {
        'hash': file_hash(filepath),
        'timestamp': datetime.now().isoformat(),
        'pages': updated_pages,
    }
    save_manifest(manifest)


def _system_prompt() -> str:
    return (
        'You maintain a wiki for the Ahold Delhaize Freshness Sprint POC. '
        'Given a source document and current wiki page contents, return JSON with this exact structure:\n'
        '{"pages": [{"name": "<PageName>", "full_markdown_content": "<full page content>"}]}\n'
        'Merge new knowledge into existing pages. Preserve prior content; add, correct, cross-reference as needed. '
        'Page names must be one of: Architecture-Overview, Data-Integration, ML-Models, '
        'Replenishment-Engine, SAP-Integration, Frontend, Infrastructure, Index. '
        'Only include pages that need updating. Return valid JSON only, no markdown fences.'
    )


def _format_existing_pages(pages: dict) -> str:
    if not pages:
        return '(no existing pages yet)'
    return '\n\n'.join(f'=== {name}.md ===\n{content}' for name, content in pages.items())


def check_and_ingest_all(client: anthropic.Anthropic) -> int:
    manifest = load_manifest()
    count = 0
    for ext in SUPPORTED_EXTENSIONS | IMAGE_EXTENSIONS:
        for filepath in RESEARCH_DIR.rglob(f'*{ext}'):
            old_manifest = dict(manifest)
            ingest_file(filepath, manifest, client)
            if str(filepath) in manifest and manifest[str(filepath)] != old_manifest.get(str(filepath)):
                count += 1
    return count


def main() -> None:
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print('ANTHROPIC_API_KEY not set', file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    if len(sys.argv) == 2 and sys.argv[1] == '--check':
        count = check_and_ingest_all(client)
        if count:
            print(f'Wiki: ingested {count} new/changed file(s)')
    elif len(sys.argv) == 2:
        filepath = Path(sys.argv[1])
        manifest = load_manifest()
        if is_changed(filepath, manifest):
            ingest_file(filepath, manifest, client)
    else:
        print('Usage: ingest.py <filepath> | --check', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
