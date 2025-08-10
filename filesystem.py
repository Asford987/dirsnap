import hashlib
import pathlib
from fnmatch import fnmatch
from typing import Iterator, Optional, TypedDict


class FileEntry(TypedDict):
    relative_path: str
    path: pathlib.Path
    size: int
    time_last_update_ns: int

class FileHead(TypedDict):
    relative_path: str
    content: bytes

class FileClass(TypedDict):
    relative_path: str
    is_binary: bool

class FileHash(TypedDict):
    relative_path: str
    hash: Optional[str]
    

class MetaEntry(TypedDict, total=False):
    size: int
    time_last_update_ns: int
    hash: str | None
    is_binary: bool
    
MetaMap = dict[str, MetaEntry]


def scan_subdir(root: pathlib.Path, subdir: str) -> Iterator[FileEntry]:
    subdir_path = root / subdir
    if not subdir_path.is_dir():
        return []
    it = subdir_path.glob("**/*")
    it = filter(lambda f: f.is_file(), it)
    def to_entry(f: pathlib.Path) -> FileEntry:
        st = f.stat()
        return {
            "relative_path": f.relative_to(subdir_path).as_posix(),
            "size": int(st.st_size),
            "path": f,
            "time_last_update_ns": st.st_mtime_ns
        }
    return map(to_entry, it)

def filter_patterns(entries: Iterator[FileEntry], patterns: list[str]) -> Iterator[FileEntry]:
    if not patterns:
        return entries
    return filter(lambda item: not any(fnmatch(item["relative_path"], pat) for pat in patterns), entries) # TODO: CHANGE HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

def read_first_bytes(entries: Iterator[FileEntry], num_bytes: int) -> Iterator[FileHead]:
    def probe(e: FileEntry) -> FileHead:
        try:
            with e["path"].open("rb") as f:
                return {"relative_path": e["relative_path"], "content": f.read(num_bytes)}
        except OSError:
            # treat unreadable as empty (will classify as binary)
            return {"relative_path": e["relative_path"], "content": b""}
    return map(probe, entries)

def classify_binary_or_text(entries: Iterator[FileEntry], probe_bytes: int) -> Iterator[FileClass]:
    heads = read_first_bytes(entries, probe_bytes)
    def is_binary(chunk: bytes) -> bool:
        if not chunk or b"\x00" in chunk:
            return True
        try:
            chunk.decode("utf-8")
            return False
        except UnicodeDecodeError:
            return True
    return map(lambda h: {"relative_path": h["relative_path"], "is_binary": is_binary(h["content"])}, heads)

def hash_files(entries: Iterator[FileEntry], chunk_size: int) -> Iterator[FileHash]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    
    def do_hash(e: FileEntry) -> FileHash:
        h = hashlib.sha256()
        try:
            with e["path"].open("rb") as f:
                while chunk := f.read(chunk_size):
                    h.update(chunk)
            return {"relative_path": e["relative_path"], "hash": h.hexdigest()}
        except OSError:
            return {"relative_path": e["relative_path"], "hash": None}
    
    return map(do_hash, entries)
    
def merge_into_metadata(entries: Iterator[FileEntry], classes: Iterator[FileClass], hashes: Iterator[FileHash]) -> MetaMap:
    cls_by = dict(map(lambda c: (c["relative_path"], c["is_binary"]), classes))
    h_by   = dict(map(lambda h: (h["relative_path"], h.get("hash")), hashes))

    return dict(map(
        lambda e: (
            e["relative_path"],
            {
                "size": e["size"],
                "time_last_update_ns": e["time_last_update_ns"],
                "hash": h_by.get(e["relative_path"], None),
                "is_binary": bool(cls_by.get(e["relative_path"], True)),
            }
        ),
        entries
    ))