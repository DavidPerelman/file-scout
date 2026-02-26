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


def find_duplicates(file_list: list[dict], on_progress=None) -> dict[str, list[dict]]:  # Accept the file list and an optional callable(int) that receives the running file count
    groups: dict[str, list[dict]] = defaultdict(list)  # Map each hash to the list of file dicts that produce it

    for index, file in enumerate(file_list, start=1):  # Iterate with a 1-based index so the progress value matches the total
        full_path = Path(file["folder"]) / file["name"]  # Reconstruct the absolute file path from folder + name

        file_hash = _hash_file(full_path)  # Compute the SHA-256 hash for this file

        if file_hash is None:  # Skip files that could not be read (permission error, etc.)
            continue

        groups[file_hash].append(file)  # Add this file dict to the group for its hash

        if on_progress is not None:  # Call the progress callback only if one was provided
            on_progress(index)  # Pass the current 1-based file index so the caller can update a progress bar

    duplicates = {  # Build the final result keeping only hashes that have 2 or more files
        hash_val: files  # Key is the hex hash string, value is the list of matching file dicts
        for hash_val, files in groups.items()  # Iterate over every hash group collected
        if len(files) >= 2  # Only include groups where at least two files share the same hash
    }

    return duplicates  # Return the filtered dict of duplicate groups
