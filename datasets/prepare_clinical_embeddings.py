#!/usr/bin/env python3
"""
Clinical Embeddings Dataset Preparation Script

This script downloads clinical word embeddings from the gweissman/clinical_embeddings
repository and creates parquet files with the embeddings.

Downloads Word2Vec embeddings trained on Open Access Case Reports:
- 100D (~269 MB)
- 300D (~716 MB)  
- 600D (~1.4 GB)

Output:
- w2v_100d_oa_cr_embeddings.parquet
- w2v_300d_oa_cr_embeddings.parquet
- w2v_600d_oa_cr_embeddings.parquet

Usage:
    python prepare_clinical_embeddings.py          # Download all 3 models
    python prepare_clinical_embeddings.py --model w2v_100d_oa_cr  # Download one

Requirements:
    pip install gensim pyarrow pandas requests tqdm
"""

import os
import sys
import tarfile
import tempfile
import shutil
from pathlib import Path
from typing import Optional, List
import argparse

# Check for required packages
try:
    import requests
    from tqdm import tqdm
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gensim.models import Word2Vec, KeyedVectors
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Install with: pip install gensim pyarrow pandas requests tqdm")
    sys.exit(1)


# Download URLs from https://github.com/gweissman/clinical_embeddings
# Using Box.com shared links - Word2Vec models only
DOWNLOAD_URLS = {
    # Word2Vec - Open Access Case Reports (main models for benchmarking)
    "w2v_100d_oa_cr": "https://upenn.box.com/shared/static/6sqzqvcunar39324adgy8qncm7yam6hu.gz",
    "w2v_300d_oa_cr": "https://upenn.box.com/shared/static/s52hsf65c51e3ro0ssx79e6l25qykt0m.gz",
    "w2v_600d_oa_cr": "https://upenn.box.com/shared/static/3y4h8iwg1dg2y3dqdwufspsl61usc0xv.gz",
    
    # Word2Vec - Open Access All Manuscripts (larger corpus)
    "w2v_100d_oa_all": "https://upenn.box.com/shared/static/gkyqs962i3i2rw55a821n62ex410bi4a.gz",
    "w2v_300d_oa_all": "https://upenn.box.com/shared/static/9djgjigsve09a7f9vz6ubtsovqwb40xa.gz",
}

# Default models to download (all Case Reports dimensions)
DEFAULT_MODELS = ["w2v_100d_oa_cr", "w2v_300d_oa_cr", "w2v_600d_oa_cr"]


def download_file(url: str, dest_path: Path, desc: str = "Downloading") -> None:
    """Download a file with progress bar."""
    response = requests.get(url, stream=True, allow_redirects=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    block_size = 8192
    
    with open(dest_path, 'wb') as f:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=desc) as pbar:
            for chunk in response.iter_content(chunk_size=block_size):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))


def extract_tar_gz(tar_path: Path, extract_dir: Path) -> Path:
    """Extract a tar.gz file and return path to main extracted file."""
    print(f"Extracting {tar_path}...")
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(extract_dir)
        # Find the .bin file
        for member in tar.getmembers():
            if member.name.endswith('.bin'):
                return extract_dir / member.name
    
    # Return first file if no .bin found
    for f in extract_dir.iterdir():
        if f.is_file():
            return f
    raise FileNotFoundError("No model file found in archive")


def load_word2vec_model(model_path: Path) -> dict:
    """Load a Word2Vec model and return word -> vector dictionary."""
    print(f"Loading Word2Vec model from {model_path}...")
    
    try:
        # Try loading as full Word2Vec model first
        model = Word2Vec.load(str(model_path))
        wv = model.wv
    except Exception:
        try:
            # Try loading as KeyedVectors
            wv = KeyedVectors.load(str(model_path))
        except Exception:
            # Try loading as word2vec text format
            wv = KeyedVectors.load_word2vec_format(str(model_path), binary=True)
    
    print(f"Loaded {len(wv.key_to_index)} words, {wv.vector_size} dimensions")
    return wv


def embeddings_to_parquet(wv, output_path: Path) -> None:
    """Convert word embeddings to parquet format."""
    print(f"Converting embeddings to parquet format...")
    
    words = list(wv.key_to_index.keys())
    vectors = [wv[word].tolist() for word in words]
    
    # Create DataFrame
    df = pd.DataFrame({
        'word': words,
        'vector': vectors,
        'dimension': [wv.vector_size] * len(words)
    })
    
    # Save to parquet with compression
    df.to_parquet(output_path, compression='snappy', index=False)
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"Saved {len(words)} word embeddings to {output_path} ({file_size_mb:.1f} MB)")


def prepare_clinical_embeddings(
    model_key: str,
    output_dir: Optional[Path] = None,
    cache_dir: Optional[Path] = None,
) -> Path:
    """
    Download clinical embeddings and convert to parquet format.
    
    Args:
        model_key: Which model to download (e.g., 'w2v_100d_oa_cr')
        output_dir: Directory for output parquet file
        cache_dir: Directory to cache downloaded files
    
    Returns:
        Path to the output parquet file
    """
    if model_key not in DOWNLOAD_URLS:
        raise ValueError(f"Unknown model: {model_key}. Available: {list(DOWNLOAD_URLS.keys())}")
    
    # Setup directories
    if output_dir is None:
        output_dir = Path.cwd()
    if cache_dir is None:
        cache_dir = Path.home() / ".cache" / "clinical_embeddings"
    
    output_dir = Path(output_dir)
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"{model_key}_embeddings.parquet"
    
    # Check if output already exists
    if output_file.exists():
        print(f"Output file already exists: {output_file}")
        return output_file
    
    # Download the model archive
    url = DOWNLOAD_URLS[model_key]
    archive_path = cache_dir / f"{model_key}.tar.gz"
    
    if not archive_path.exists():
        print(f"Downloading {model_key} embeddings...")
        download_file(url, archive_path, desc=f"Downloading {model_key}")
    else:
        print(f"Using cached archive: {archive_path}")
    
    # Extract the archive
    extract_dir = cache_dir / model_key
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)
    
    model_path = extract_tar_gz(archive_path, extract_dir)
    
    # Load the model
    wv = load_word2vec_model(model_path)
    
    # Convert to parquet
    embeddings_to_parquet(wv, output_file)
    
    # Cleanup extracted files (keep archive for caching)
    shutil.rmtree(extract_dir)
    
    return output_file


def main():
    parser = argparse.ArgumentParser(
        description="Download clinical word embeddings and convert to parquet format"
    )
    parser.add_argument(
        "--model", "-m",
        default=None,
        choices=list(DOWNLOAD_URLS.keys()),
        help="Download a specific model (default: download all 3 case reports models)"
    )
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Download all 3 case reports models (100D, 300D, 600D) - this is the default"
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        default=None,
        help="Output directory for parquet files (default: current directory)"
    )
    parser.add_argument(
        "--cache-dir", "-c",
        type=Path,
        default=None,
        help="Cache directory for downloaded files (default: ~/.cache/clinical_embeddings)"
    )
    parser.add_argument(
        "--list-models",
        action="store_true",
        help="List available models and exit"
    )
    
    args = parser.parse_args()
    
    if args.list_models:
        print("Available models:")
        print("-" * 60)
        for key in sorted(DOWNLOAD_URLS.keys()):
            parts = key.split('_')
            model_type = parts[0]
            dims = parts[1]
            source = '_'.join(parts[2:])
            default_marker = " [DEFAULT]" if key in DEFAULT_MODELS else ""
            print(f"  {key:25s} - {model_type.upper()} {dims}, {source.replace('_', ' ').upper()}{default_marker}")
        return
    
    # Determine which models to download
    if args.model:
        models_to_download = [args.model]
    else:
        # Default: download all 3 case reports models
        models_to_download = DEFAULT_MODELS
        print(f"Downloading all {len(models_to_download)} case reports models (100D, 300D, 600D)...")
        print("Use --model to download a specific model.")
        print()
    
    try:
        for i, model_key in enumerate(models_to_download):
            print(f"\n{'='*60}")
            print(f"[{i+1}/{len(models_to_download)}] Processing {model_key}")
            print(f"{'='*60}")
            output_file = prepare_clinical_embeddings(
                model_key=model_key,
                output_dir=args.output_dir,
                cache_dir=args.cache_dir,
            )
            print(f"[OK] Created: {output_file}")
        
        print(f"\n{'='*60}")
        print(f"[OK] Successfully created {len(models_to_download)} parquet file(s)")
        print(f"{'='*60}")
    except Exception as e:
        print(f"\n[FAIL] Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

