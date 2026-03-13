"""Arxiv metadata parsing and retrieval module."""

import re
from typing import Any

import arxiv


def parse_arxiv_id_from_text(text: str) -> str | None:
    """Parse Arxiv ID from a string using regex."""
    # Pattern for Arxiv IDs: 2301.00001 or 0704.0001
    pattern = r"(\d{4}\.\d{4,5})"
    match = re.search(pattern, text)
    return match.group(1) if match else None


def get_arxiv_metadata(arxiv_id: str) -> dict[str, Any]:
    """Fetch metadata for a paper from Arxiv using its ID."""
    client = arxiv.Client()
    search = arxiv.Search(id_list=[arxiv_id])

    results = list(client.results(search))
    if not results:
        raise ValueError(f"No Arxiv paper found for ID: {arxiv_id}")

    result = results[0]

    return {
        "id": arxiv_id,
        "title": result.title,
        "authors": [author.name for author in result.authors],
        "summary": result.summary,
        "published": result.published,
        "updated": result.updated,
        "links": [link.href for link in result.links],
        "categories": result.categories,
    }
