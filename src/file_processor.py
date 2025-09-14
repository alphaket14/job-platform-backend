"""
File processing utilities for parsing job feeds.
This version contains both the original "smart" parser for the pipeline
and a new "raw" parser for the inspector tool.
"""
import itertools
import xml.etree.ElementTree as ET
import logging
import json
import os
import gzip
import zlib
import re
import html
import codecs
from typing import Dict, Any, List, Iterator

logger = logging.getLogger(__name__)



def _get_child_text(element: ET.Element, tag_name: str) -> str | None:
    """Case-insensitively finds a child element and returns its stripped text."""
    child = next((c for c in element if hasattr(c, 'tag') and c.tag.lower() == tag_name.lower()), None)
    return child.text.strip() if child is not None and child.text else None


def _extract_job_data(job_element: ET.Element, mapping_config: Dict) -> Dict[str, Any]:
    """
    Extracts and maps data for a single job XML element, safely unescaping any
    values that are URLs.
    """
    job_data = {}
    mappings = mapping_config.get('mappings', {})

    for source_key, target_field in mappings.items():
        if isinstance(target_field, str):
            value = None

            child = next((c for c in job_element if hasattr(c, 'tag') and c.tag.lower() == source_key.lower()), None)
            if child is not None and child.text:
                value = child.text.strip()

            if value is None:
                for attr_key, attr_value in job_element.attrib.items():
                    if attr_key.lower() == source_key.lower():
                        value = attr_value
                        break

            if value:
                if isinstance(value, str) and re.match(r'^https?://', value):
                    job_data[target_field] = html.unescape(value)
                else:
                    job_data[target_field] = value
    return job_data


def _extract_raw_job_data(job_element: ET.Element) -> Dict[str, Any]:
    """Extracts all child tags and attributes, safely unescaping any values that are URLs."""
    job_data = {}
    if job_element is not None:
        for key, value in job_element.attrib.items():
            if isinstance(value, str) and re.match(r'^https?://', value):
                job_data[key] = html.unescape(value)
            else:
                job_data[key] = value

        for child in job_element:
            value = child.text.strip() if child.text else ""
            if isinstance(value, str) and re.match(r'^https?://', value):
                job_data[child.tag] = html.unescape(value)
            else:
                job_data[child.tag] = value

    return job_data


def _stream_xml_jobs(text_iterator: Iterator[str], chunk_size: int, extractor_func, mapping_config=None) -> Iterator[List[Dict[str, Any]]]:
    """
    Core XML streaming parser. It takes any iterator yielding text, finds <job>
    elements, runs the provided extractor function, and yields them in batches.
    """
    buffer = ""
    job_count = 0
    current_chunk = []
    for text_chunk in text_iterator:
        buffer += text_chunk
        while '<job>' in buffer and '</job>' in buffer:
            start_index = buffer.find('<job>')
            end_index = buffer.find('</job>') + 6
            job_xml = buffer[start_index : end_index]
            buffer = buffer[end_index:]
            try:
                job_xml = job_xml.replace('\x00', '')
                job_xml = re.sub(r'&(?!(?:amp|lt|gt|quot|apos);)', '&amp;', job_xml)
                job_element = ET.fromstring(job_xml)

                job_data = extractor_func(job_element, mapping_config) if mapping_config else extractor_func(job_element)

                if job_data:
                    current_chunk.append(job_data)
                    job_count += 1
                    if job_count > 0 and job_count % 1000 == 0:
                        logger.info(f"Streamed and parsed {job_count} jobs from source...")
                if len(current_chunk) >= chunk_size:
                    yield current_chunk
                    current_chunk = []
            except ET.ParseError as e:
                logger.warning(f"Skipping malformed job XML fragment. Error: {e}")
                continue
    if current_chunk:
        yield current_chunk

def _stream_from_response(response, chunk_size: int, track_bytes: bool, extractor_func, mapping_config=None) -> Iterator:
    """
    Generic helper to parse an XML stream from an HTTP response, handling gzip
    and yielding job chunks using the specified extractor.
    """
    content_iterator = response.iter_content(chunk_size=8192)
    try:
        first_chunk = next(content_iterator)
    except StopIteration:
        return iter([])
    full_iterator = itertools.chain([first_chunk], content_iterator)
    is_gzipped = first_chunk.startswith(b'\x1f\x8b')
    if is_gzipped:
        logger.info("Gzip content detected by magic number in the stream.")
    decompressor = zlib.decompressobj(32 + zlib.MAX_WBITS) if is_gzipped else None
    decoder = codecs.getincrementaldecoder('utf-8')()
    total_bytes_read = [0] # Use a list to make it mutable inside the closure
    def text_iterator(binary_iterator):
        for chunk in binary_iterator:
            if track_bytes: total_bytes_read[0] += len(chunk)
            if decompressor: chunk = decompressor.decompress(chunk)
            if chunk: yield decoder.decode(chunk, final=False)
        final_chunk = decompressor.flush() if decompressor else b''
        yield decoder.decode(final_chunk, final=True)

    stream_generator = _stream_xml_jobs(text_iterator(full_iterator), chunk_size, extractor_func, mapping_config)
    for job_chunk in stream_generator:
        yield (job_chunk, total_bytes_read[0]) if track_bytes else job_chunk
    return None


def parse_xml_stream(file_path: str, chunk_size: int = 100, mapping_config: dict = None) -> Iterator[List[Dict[str, Any]]]:
    """
    Parses a local XML file in a streaming fashion, handling .gz compression automatically.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    is_compressed = file_path.endswith('.gz')
    file_opener = gzip.open if is_compressed else open
    try:
        with file_opener(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
            yield from _stream_xml_jobs(f, chunk_size, _extract_job_data, mapping_config)
    except Exception as e:
        logger.error(f"Failed to stream or parse local XML file {file_path}: {e}", exc_info=True)
        raise

def parse_xml_stream_from_response(response, chunk_size: int = 100, track_bytes: bool = False, mapping_config: dict = None) -> Iterator:
    """
    Parses an XML stream from an HTTP response for the main pipeline (applies mappings).
    """
    yield from _stream_from_response(response, chunk_size, track_bytes, _extract_job_data, mapping_config)

def parse_raw_xml_stream_from_response(response, chunk_size: int = 100) -> Iterator[List[Dict[str, Any]]]:
    """
    Parses an XML stream from an HTTP response for the inspector tool (preserves raw tags).
    """
    yield from _stream_from_response(response, chunk_size, False, _extract_raw_job_data, None)