"""Refusal check for the clinical archive: run with `python3 datasets/test_prepare_clinical_embeddings.py`. Stdlib only, no network."""

import io
import sys
import tarfile
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import prepare_clinical_embeddings as prep

MODEL = "w2v_100d_oa_cr"


def write_archive(path: Path, member: str) -> None:
    data = b"pickled model"
    with tarfile.open(path, "w:gz") as tar:
        info = tarfile.TarInfo(member)
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))


def run(root: Path, member: str, pin_of, digest_refused: bool = True) -> list:
    cache = root / "cache"
    cache.mkdir()
    archive = cache / f"{MODEL}.tar.gz"
    write_archive(archive, member)
    loads = []
    prep.load_word2vec_model = lambda path: loads.append(path)
    prep.embeddings_to_parquet = lambda wv, out: out.write_bytes(b"")
    try:
        prep.prepare_clinical_embeddings(MODEL, output_dir=root / "out", cache_dir=cache, archive_sha256=pin_of(archive))
    except prep.ArchiveIntegrityError:
        assert loads == [], "a refused archive was loaded"
        assert archive.exists() is (digest_refused is False), "a digest refusal deletes the archive, a member refusal keeps the verified one"
        return loads
    raise AssertionError(f"the archive with member '{member}' was accepted")


def main() -> None:
    with tempfile.TemporaryDirectory() as root:
        run(Path(root), "model.bin", lambda archive: "0" * 64)
    with tempfile.TemporaryDirectory() as root:
        run(Path(root), "model.bin", lambda archive: None)
    with tempfile.TemporaryDirectory() as root:
        run(Path(root), "../escape.bin", prep.sha256_of, digest_refused=False)
        assert (Path(root) / "cache" / "escape.bin").exists() is False, "a '..' member was written outside the extraction directory"
    with tempfile.TemporaryDirectory() as root:
        loads = []
        cache = Path(root) / "cache"
        cache.mkdir()
        archive = cache / f"{MODEL}.tar.gz"
        write_archive(archive, "model.bin")
        prep.load_word2vec_model = lambda path: loads.append(path.name)
        prep.prepare_clinical_embeddings(MODEL, output_dir=Path(root) / "out", cache_dir=cache, archive_sha256=prep.sha256_of(archive))
        assert loads == ["model.bin"], "a verified archive is loaded"
    print("ok")


if __name__ == "__main__":
    main()
