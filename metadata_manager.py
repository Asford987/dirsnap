from collections import deque
from datetime import datetime
from itertools import chain, groupby
import json
import pathlib
from typing import TypedDict

from filesystem import MetaMap

class MetaDiffs(TypedDict):
    added: list[str]
    removed: list[str]
    moved: list[tuple[str, str]]
    modified: list[str]


def load_metadata(metadata_path: pathlib.Path, metadata_filetype: str) -> MetaMap:
    if metadata_filetype.lower() != "json":
        raise ValueError(f"unsupported metadata_filetype: {metadata_filetype}")

    if not metadata_path.exists():
        return {}

    with metadata_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def compute_diffs(old_data: MetaMap, new_data: MetaMap) -> MetaDiffs:
    old_paths = set(old_data.keys())
    new_paths = set(new_data.keys())

    added0   = new_paths - old_paths
    removed0 = old_paths - new_paths

    def index_by_hash(data: MetaMap, paths: set[str]) -> dict[str, list[str]]:
        pairs = map(lambda p: (data[p].get("hash"), p), paths)
        non_null = list(filter(lambda kv: kv[0] is not None, pairs))
        # groupby needs sorting by key (hash)
        grouped = groupby(sorted(non_null, key=lambda kv: kv[0]), key=lambda kv: kv[0])
        return dict(
            map(
                lambda grp: (grp[0], list(map(lambda kv: kv[1], grp[1]))),
                grouped
            )
        )

    added_by_hash   = index_by_hash(new_data, added0)
    removed_by_hash = index_by_hash(old_data, removed0)

    common_hashes = sorted(set(added_by_hash.keys()) & set(removed_by_hash.keys()))

    moves_iter = map(
        lambda h: list(zip(sorted(removed_by_hash[h]), sorted(added_by_hash[h]))),
        common_hashes
    )
    moves_list = list(chain.from_iterable(moves_iter))

    moved_old = set(map(lambda t: t[0], moves_list))
    moved_new = set(map(lambda t: t[1], moves_list))

    added   = sorted(added0 - moved_new)
    removed = sorted(removed0 - moved_old)
    moved   = moves_list  # already a list

    common = old_paths & new_paths

    def is_modified(p: str) -> bool:
        oh = old_data[p].get("hash")
        nh = new_data[p].get("hash")
        both_hashes = (oh is not None) and (nh is not None)
        return (both_hashes and oh != nh) or (not both_hashes and old_data[p].get("size") != new_data[p].get("size"))

    modified = sorted(filter(is_modified, common))

    return {
        "added": added,
        "removed": removed,
        "moved": moved,
        "modified": modified,
    }
  

def save_metadata(metadata: MetaMap, metadata_path: pathlib.Path, metadata_filetype: str):
    ft = metadata_filetype.lower()
    if ft != "json":
        raise ValueError(f"unsupported metadata_filetype: {metadata_filetype}")

    tmp = metadata_path.with_suffix(metadata_path.suffix + ".tmp")
    tmp.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(metadata_path)
    
    
def print_changes(diffs: MetaDiffs, verbosity: int, subdir: pathlib.Path):
    match verbosity:
        case 0:
            print_changes_v0(diffs)
        case 1:
            print_changes_v1(diffs, subdir)
        case _:
            raise ValueError(f"verbosity value not supported: {verbosity}")

def print_changes_v0(diffs: MetaDiffs):
    emit = print
    consume = lambda it: deque(it, maxlen=0)
    consume(chain(
        map(lambda p: emit(f"A {p}"), sorted(diffs["added"])),
        map(lambda p: emit(f"M {p}"), sorted(diffs["modified"])),
        map(lambda t: emit(f"R {t[0]} -> {t[1]}"), sorted(diffs["moved"])),
        map(lambda p: emit(f"D {p}"), sorted(diffs["removed"])),
    ))
 

def print_changes_v1(diffs: MetaDiffs, subdir: pathlib.Path):
    emit = print
    consume = lambda it: deque(it, maxlen=0)

    header = lambda: consume((
        emit(f"Directory: {subdir.resolve().as_posix()}"),
        emit(f"Time: {datetime.now().isoformat()}"),
        emit(""),
    ) for _ in [None])

    def section(title: str, items):
        return chain(
            (emit(title) for _ in [None]) if items else (),
            map(lambda x: emit(f"\t* {x[0]} -> {x[1]}") if isinstance(x, tuple) else emit(f"\t* {x}"),
                items),
            (emit(""),) if items else ()
        )

    body = chain(
        section("Added",          sorted(diffs["added"])),
        section("Modified",       sorted(diffs["modified"])),
        section("Renamed/Moved",  sorted(diffs["moved"])),
        section("Deleted/Removed",sorted(diffs["removed"])),
    )

    counts = (
        len(diffs["added"]),
        len(diffs["modified"]),
        len(diffs["moved"]),
        len(diffs["removed"]),
    )
    total = sum(counts)

    summary = lambda: consume((
        emit("Summary:"),
        emit(f"{counts[0]} added, "
             f"{counts[1]} modified, "
             f"{counts[2]} moved, "
             f"{counts[3]} removed (total {total})"),
    ) for _ in [None])

    header()
    consume(body)      # realize body prints
    summary()