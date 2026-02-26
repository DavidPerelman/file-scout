from pathlib import Path  # Import Path for clean, cross-platform filesystem access
from datetime import datetime  # Import datetime to convert modification timestamps
from typing import Generator  # Import Generator to annotate the yield-based return type


def scan_folder(path: str) -> Generator[dict, None, None]:  # Yield one file-info dict at a time so callers can process files incrementally
    folder = Path(path)  # Convert the string path to a Path object for easy manipulation

    if not folder.is_dir():  # Check that the given path is an existing directory
        raise NotADirectoryError(f"'{path}' is not a valid directory")  # Raise a clear error if it isn't

    for entry in folder.rglob("*"):  # Recursively walk all items inside the root folder
        if not entry.is_file():  # Skip subdirectories, symlinks to dirs, and other non-file entries
            continue  # Move on to the next entry

        stat = entry.stat()  # Read the file's filesystem metadata (size, timestamps, etc.)

        name = entry.name  # The file's full name including extension (e.g. "report.pdf")
        size_bytes = stat.st_size  # File size in bytes as reported by the OS
        file_type = entry.suffix.lstrip(".").lower()  # Extension without the dot, lowercased (e.g. "pdf"); empty string if none
        modified_ts = stat.st_mtime  # Last-modified time as a Unix timestamp (float)
        modified_date = datetime.fromtimestamp(modified_ts)  # Convert Unix timestamp to a datetime object; caller decides how to format it
        parent_folder = str(entry.parent)  # Absolute path of the directory that directly contains this file

        yield {  # Yield the file dict immediately so the caller receives it without waiting for the full scan to finish
            "name": name,  # File name
            "size_bytes": size_bytes,  # Raw size in bytes (caller can format as KB/MB as needed)
            "file_type": file_type,  # Extension string (empty string for files with no extension)
            "modified_date": modified_date,  # datetime object; format it however you need in the caller
            "folder": parent_folder,  # Full path to the parent directory so the caller knows where the file lives
        }
