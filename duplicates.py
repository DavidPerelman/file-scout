import hashlib  # Import hashlib to compute cryptographic hash digests
from pathlib import Path  # Import Path to construct full file paths from folder + name
from collections import defaultdict  # Import defaultdict to group files by hash without manual key checks


def _hash_file(file_path: Path) -> str | None:  # Compute the SHA-256 hash of a single file; returns None if the file can't be read
    hasher = hashlib.sha256()  # Create a new SHA-256 hash object

    try:
        with open(file_path, "rb") as f:  # Open the file in binary mode so all file types are handled correctly
            while chunk := f.read(65536):  # Read the file in 64 KB chunks to avoid loading large files into memory at once
                hasher.update(chunk)  # Feed each chunk into the hasher to update the running digest
    except (OSError, PermissionError):  # Catch read errors (e.g. locked files, permission denied, broken symlinks)
        return None  # Return None so the caller can skip this file gracefully

    return hasher.hexdigest()  # Return the final hash as a lowercase hex string (64 characters)


def find_duplicates(
    file_list: list[dict],
    on_progress=None,   # Optional callable(int) that receives the 1-based file index; called for every file so the bar fills correctly
    stop_event=None,    # Optional threading.Event; checked before each file so the caller can interrupt hashing early
) -> dict[str, list[dict]]:

    # --- Phase 1: group file indices by size ---
    # Two files can only be duplicates if they are the same size, so any file
    # whose size is unique across the entire list can be skipped without hashing.
    by_size: dict[int, list[int]] = defaultdict(list)  # size_bytes → [0-based indices]
    for i, file in enumerate(file_list):
        by_size[file["size_bytes"]].append(i)

    # Build the candidate set: every 0-based index that belongs to a size group
    # containing at least two files.  Only these files need to be hashed.
    candidate_indices: set[int] = {
        i
        for indices in by_size.values()
        if len(indices) >= 2
        for i in indices
    }

    # --- Phase 2: hash candidates; advance progress for every file ---
    # on_progress is still called for every file (including non-candidates) so the
    # progress bar maximum — set by the caller to len(file_list) — is reached
    # exactly, keeping the bar accurate regardless of how many files are skipped.
    groups: dict[str, list[dict]] = defaultdict(list)

    for index, file in enumerate(file_list, start=1):  # index is 1-based to match the bar maximum
        if stop_event is not None and stop_event.is_set():  # Honour a stop request before touching the next file
            break

        if (index - 1) in candidate_indices:  # index is 1-based; candidate_indices uses 0-based positions
            full_path = Path(file["folder"]) / file["name"]  # Reconstruct the absolute path from folder + name
            file_hash = _hash_file(full_path)  # Compute SHA-256; returns None if the file can't be read
            if file_hash is not None:
                groups[file_hash].append(file)  # Add to the group for this hash

        if on_progress is not None:
            on_progress(index)  # Always advance the bar, even for files that were skipped

    return {  # Return only groups where at least two files share the same hash
        hash_val: files
        for hash_val, files in groups.items()
        if len(files) >= 2
    }
