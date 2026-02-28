import sys   # Import sys to access command-line arguments and exit functionality
import os    # Import os for os.startfile() to open files with their default application
from PyQt6.QtWidgets import (  # Import all needed Qt widgets
    QApplication, QMainWindow, QWidget,  # Core window and container widgets
    QVBoxLayout, QHBoxLayout,  # Vertical and horizontal layout managers
    QPushButton, QLabel,  # Button and text label widgets
    QTableWidget, QTableWidgetItem,  # Table widget and its cell item class
    QHeaderView, QFileDialog,  # Header behavior control and folder picker dialog
    QTabWidget, QProgressBar, QMessageBox,  # Tab container, progress bar, and confirmation dialog widgets
    QComboBox, QLineEdit,  # Dropdown selector and single-line text input for the filter bar
    QListWidget,  # List widget used in the cleanup tab to display the selected root paths
    QSpinBox,  # Integer spinbox used in the cleanup tab for the large-file size threshold
    QScrollArea,  # Scrollable viewport that wraps the three cleanup result sections
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal  # Import Qt namespace, thread class, and signal type
from PyQt6.QtGui import (  # Import GUI-level classes
    QIntValidator,  # Restricts a QLineEdit to integer input only
    QColor,         # Used to set the yellow highlight background on matching filename cells
    QBrush,         # Wraps QColor into a brush accepted by QTableWidgetItem.setBackground()
)
from datetime import datetime, timedelta  # Import datetime for date comparisons; timedelta for the "older than N months" cutoff
from pathlib import Path  # Import Path for the module-level _walk_clean helper used by CleanupWorker
from send2trash import send2trash  # Import send2trash to move files to the Recycle Bin instead of permanently deleting them
from scanner import scan_folder  # Import the folder scanning generator from scanner.py
from duplicates import find_duplicates  # Import the duplicate detection function from duplicates.py


# ---------------------------------------------------------------------------
# Background workers
# ---------------------------------------------------------------------------

class ScanWorker(QThread):  # Worker that runs scan_folder() on a background thread so the UI stays responsive
    total_files = pyqtSignal(int)  # Emitted once with the total file count before scanning begins, so the progress bar can set its maximum
    progress    = pyqtSignal(int)  # Emitted after each file is found; carries the 1-based running count for setValue()
    finished    = pyqtSignal(list) # Emitted with the complete file list when the scan is done
    error       = pyqtSignal(str)  # Emitted with an error message string if an exception is raised

    def __init__(self, folder_path: str):  # Accept the folder path at construction time
        super().__init__()  # Initialize the parent QThread
        self._folder_path = folder_path  # Store the path so run() can access it

    def run(self):  # Qt calls this method in the background thread when start() is invoked
        try:
            from pathlib import Path  # Import Path here to count files before the generator runs
            total = sum(1 for e in Path(self._folder_path).rglob("*") if e.is_file())  # Count every file in the tree upfront so we can show real percentages
            self.total_files.emit(total)  # Send the total to the main thread so it can set the progress bar maximum

            files = []  # Accumulate file dicts as they arrive from the generator
            for file in scan_folder(self._folder_path):  # Iterate the generator one file at a time
                files.append(file)  # Add the file dict to the growing list
                self.progress.emit(len(files))  # Emit the current count; main thread calls setValue() with this value
            self.finished.emit(files)  # Send the complete list to the main thread when all files are found
        except Exception as e:  # Catch any unexpected error (permission denied, OS error, etc.)
            self.error.emit(str(e))  # Emit the error message so the main thread can display it


class DuplicatesWorker(QThread):  # Worker that runs find_duplicates() on a background thread so the UI stays responsive
    total_files = pyqtSignal(int)  # Emitted once with the total file count before hashing begins, so the progress bar can set its maximum
    progress    = pyqtSignal(int)  # Emitted after each file is hashed; carries the 1-based running count for setValue()
    finished    = pyqtSignal(dict) # Emitted with the duplicates dict when hashing completes successfully
    error       = pyqtSignal(str)  # Emitted with an error message string if an exception is raised

    def __init__(self, file_list: list[dict]):  # Accept the scanned file list at construction time
        super().__init__()  # Initialize the parent QThread
        self._file_list = file_list  # Store the file list so run() can access it

    def run(self):  # Qt calls this method in the background thread when start() is invoked
        try:
            self.total_files.emit(len(self._file_list))  # Total is known upfront from the list length; no pre-count pass needed
            result = find_duplicates(self._file_list, on_progress=self.progress.emit)  # Pass the progress signal's emit as the callback so each hashed file increments the bar
            self.finished.emit(result)  # Send the result back to the main thread via signal
        except Exception as e:  # Catch any unexpected error (permission denied, OS error, etc.)
            self.error.emit(str(e))  # Emit the error message so the main thread can display it


class CleanupWorker(QThread):  # Worker that scans one or more root paths, then hashes for duplicates, entirely on the background thread
    total_files   = pyqtSignal(int)  # Emitted once before scanning begins with the combined file count, so the progress bar can set its maximum
    progress      = pyqtSignal(int)  # Emitted after each file is discovered during the scan pass; drives progress bar + status label
    hash_start    = pyqtSignal(int)  # Emitted once before hashing begins with len(files) as the new bar maximum; resets the bar for the hash phase
    hash_progress = pyqtSignal(int)  # Emitted after each file is hashed; drives only progress bar setValue — never touches the status label
    finished      = pyqtSignal(list, dict)  # Emitted with (file_list, duplicates_dict) when both scan and hash passes complete
    error         = pyqtSignal(str)  # Emitted with an error message string if any unrecoverable exception is raised

    def __init__(self, roots: list[str]):  # Accept the list of root paths chosen in the multi-root picker
        super().__init__()  # Initialise the parent QThread
        self._roots = roots  # Store the root list so run() can access it; never mutated after construction

    def run(self):  # Qt calls this in the background thread when start() is invoked
        try:
            # --- Pre-count pass: use _walk_clean so the count matches what the scan pass will actually visit ---
            total = sum(
                sum(1 for _ in _walk_clean(Path(root)))  # Count accessible, non-skipped files under this root
                for root in self._roots
            )
            self.total_files.emit(total)  # Send the combined total so the progress bar switches from indeterminate to percentage mode

            # --- Scan pass: iterate _walk_clean for every root and build file dicts on the fly ---
            files = []  # Flat list that collects file dicts from all roots in sequence
            for root in self._roots:
                for entry in _walk_clean(Path(root)):  # Same skip-aware, PermissionError-safe walk as the count pass
                    try:
                        stat = entry.stat()  # Read filesystem metadata; may raise OSError if the file vanished between discovery and stat
                    except OSError:
                        continue  # Skip any file that can no longer be read without aborting the whole scan
                    files.append({  # Build the same file-dict shape that scanner.py produces so all downstream code stays compatible
                        "name":          entry.name,
                        "size_bytes":    stat.st_size,
                        "file_type":     entry.suffix.lstrip(".").lower(),
                        "modified_date": datetime.fromtimestamp(stat.st_mtime),
                        "folder":        str(entry.parent),
                    })
                    self.progress.emit(len(files))  # Advance the progress bar one step per file

            # --- Hash pass: find duplicates among the collected files on this thread so the main thread stays responsive ---
            self.hash_start.emit(len(files))  # Signal the main thread to reset the progress bar for the hash phase
            duplicates = find_duplicates(files, on_progress=self.hash_progress.emit)  # Reuse duplicates.py; hash_progress drives only the bar
            self.finished.emit(files, duplicates)  # Deliver both the file list and the duplicates dict in a single signal

        except Exception as e:  # Catch any unexpected error not already handled inside the loops
            self.error.emit(str(e))  # Emit the message so the main thread can display it in the status bar


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_size(size_bytes: int) -> str:  # Convert raw byte count to a human-readable KB or MB string
    if size_bytes < 1_048_576:  # Less than 1 MB (1024 * 1024 bytes)
        return f"{size_bytes / 1024:.1f} KB"  # Show as kilobytes with one decimal place
    return f"{size_bytes / 1_048_576:.1f} MB"  # Otherwise show as megabytes with one decimal place


def format_total_size(total_bytes: int) -> str:  # Format the total size for the status bar
    if total_bytes < 1_048_576:  # Less than 1 MB
        return f"{total_bytes / 1024:.1f} KB"  # Show total as KB
    return f"{total_bytes / 1_048_576:.1f} MB"  # Show total as MB


FILE_TYPE_GROUPS: dict[str, set[str]] = {  # Maps each Hebrew category label to its set of lowercase extensions
    "תמונות": {"jpg", "jpeg", "png", "gif", "bmp", "webp", "tiff", "tif", "svg", "ico", "heic", "heif", "raw"},
    "מסמכים": {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "rtf", "odt", "ods", "odp", "csv", "md"},
    "וידאו":  {"mp4", "avi", "mkv", "mov", "wmv", "flv", "webm", "m4v", "mpeg", "mpg"},
    "מוזיקה": {"mp3", "wav", "flac", "aac", "ogg", "wma", "m4a", "opus", "aiff"},
}

# ---------------------------------------------------------------------------
# Cleanup walk helpers
# ---------------------------------------------------------------------------

# Directory path prefixes that CleanupWorker never recurses into (case-insensitive).
# These are system-owned trees where user storage savings are impossible or dangerous.
_CLEANUP_SKIP_PREFIXES: tuple[str, ...] = (
    r"C:\Windows",                       # OS installation files
    r"C:\$Recycle.Bin",                  # Recycle Bin staging area
    r"C:\System Volume Information",     # System restore / VSS snapshots
    r"C:\Program Files",                 # 64-bit application installs
    r"C:\Program Files (x86)",           # 32-bit application installs (covers both with the prefix check)
)

# Substrings that cause a directory to be skipped wherever they appear in its path.
_CLEANUP_SKIP_SUBSTRINGS: tuple[str, ...] = (
    r"\AppData\Local\Temp",  # Per-user temp files managed by the OS; safe to ignore, risky to delete
)


def _walk_clean(root: Path):  # Yield Path objects for every accessible file under root, honouring the skip lists
    stack = [root]  # Iterative depth-first walk; start with the root so we never hit Python's recursion limit
    while stack:  # Process directories until none remain
        current = stack.pop()  # Take the next directory off the stack
        try:
            for entry in current.iterdir():  # List the immediate contents of this directory
                if entry.is_file():  # Plain files are yielded immediately for the caller to process
                    yield entry
                elif entry.is_dir():  # Subdirectories are only queued if they pass the skip checks
                    s = str(entry).lower()  # Lowercase once so every comparison below is case-insensitive
                    skip = (
                        any(s.startswith(p.lower()) for p in _CLEANUP_SKIP_PREFIXES)    # Matches a protected root prefix
                        or any(sub.lower() in s for sub in _CLEANUP_SKIP_SUBSTRINGS)    # Contains a protected substring anywhere in the path
                    )
                    if not skip:
                        stack.append(entry)  # Safe to recurse — add to the work queue
        except OSError:  # Covers PermissionError and any other OS-level access failure; skip the directory silently
            pass


def _make_table(headers: list[str], stretch_col: int) -> QTableWidget:  # Helper that builds a configured read-only table with given headers
    table = QTableWidget()  # Create a new table widget
    table.setColumnCount(len(headers))  # Set column count to match the number of header labels
    table.setHorizontalHeaderLabels(headers)  # Apply the provided Hebrew header labels
    for i in range(len(headers)):  # Loop over every column index
        mode = QHeaderView.ResizeMode.Stretch if i == stretch_col else QHeaderView.ResizeMode.ResizeToContents  # Stretch the designated column; others fit their content
        table.horizontalHeader().setSectionResizeMode(i, mode)  # Apply the resize mode to this column
    table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)  # Make all cells read-only
    table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)  # Full-row selection on click
    table.verticalHeader().setVisible(False)  # Hide the row number column on the left
    return table  # Return the fully configured table


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):  # Define the main window class, inheriting from QMainWindow
    def __init__(self):  # Constructor called when the window is created
        super().__init__()  # Initialize the parent QMainWindow class
        self.setWindowTitle("File Scout 🔍")  # Set the title shown in the window's title bar
        self.resize(900, 600)  # Set the initial window size to 900 wide by 600 tall (pixels)
        self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)  # Enable RTL layout for Hebrew text support

        self._scanned_files: list[dict] = []  # Cache the last scan results so find_duplicates can reuse them without re-scanning

        central_widget = QWidget()  # Create a plain widget to serve as the central container
        self.setCentralWidget(central_widget)  # Set it as the main content area of the window

        main_layout = QVBoxLayout(central_widget)  # Create a vertical layout attached to the central widget
        main_layout.setContentsMargins(8, 8, 8, 8)  # Set 8px padding around the edges of the layout
        main_layout.setSpacing(6)  # Set 6px gap between each section in the layout

        # --- Top Bar ---
        top_bar = QHBoxLayout()  # Create a horizontal layout for the top bar row

        self.select_btn = QPushButton("בחר תיקייה")  # Button to open the folder picker dialog
        self.select_btn.setFixedWidth(120)  # Fix the button width so it doesn't stretch
        self.select_btn.clicked.connect(self.choose_folder)  # Connect click to choose_folder method

        self.dup_btn = QPushButton("מצא כפילויות")  # Button to trigger duplicate detection
        self.dup_btn.setFixedWidth(120)  # Fix the button width so it doesn't stretch
        self.dup_btn.setEnabled(False)  # Disabled until a folder has been scanned
        self.dup_btn.clicked.connect(self.find_dups)  # Connect click to find_dups method

        self.path_label = QLabel("לא נבחרה תיקייה")  # Label showing the selected folder path
        self.path_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align for RTL readability

        top_bar.addWidget(self.select_btn)  # Add the folder button to the top bar
        top_bar.addWidget(self.dup_btn)  # Add the duplicates button next to it
        top_bar.addWidget(self.path_label, stretch=1)  # Path label fills the remaining space

        main_layout.addLayout(top_bar)  # Add the top bar into the main vertical layout

        # --- Progress Bar ---
        self.progress_bar = QProgressBar()  # Create the progress bar widget
        self.progress_bar.setMaximum(0)  # Start in indeterminate (pulsing) mode; maximum is updated to the real total when the worker emits total_files
        self.progress_bar.setValue(0)  # Start at zero; updated by the worker's progress signal
        self.progress_bar.setFormat("%p%")  # Show percentage text on the bar (e.g. "42%")
        self.progress_bar.setTextVisible(True)  # Make the percentage text visible on the bar
        self.progress_bar.hide()  # Hidden by default; shown only while a worker is running

        main_layout.addWidget(self.progress_bar)  # Add the progress bar below the top bar

        # --- Tab Widget ---
        self.tabs = QTabWidget()  # Create the tab container that holds both views
        self.tabs.setLayoutDirection(Qt.LayoutDirection.RightToLeft)  # Apply RTL to tab labels as well

        # Tab 1 — Scan results
        self.table = _make_table(  # Build the scan table using the shared helper
            ["שם", "גודל", "סוג", "תאריך שינוי", "תיקייה"],  # Columns: Name, Size, Type, Modified Date, Folder
            stretch_col=4,  # Stretch the Folder column to fill remaining width
        )
        self.table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)  # Disable row selection entirely so the blue highlight indicators don't appear
        self.table.cellDoubleClicked.connect(self._on_scan_row_double_clicked)  # Open the file when the user double-clicks any cell in the scan table

        # Search bar
        self.search_input = QLineEdit()  # Full-width text input for filtering rows by filename
        self.search_input.setPlaceholderText("חפש לפי שם קובץ...")  # Hint text shown when the field is empty
        self.search_input.setClearButtonEnabled(True)  # Show a built-in × button so the user can clear the search in one click
        self.search_input.textChanged.connect(self._apply_filters)  # Re-filter the visible rows on every keystroke

        search_row = QHBoxLayout()  # Horizontal row that holds the label and the search input
        search_row.addWidget(QLabel("חיפוש:"))  # Label identifies the purpose of the input field
        search_row.addWidget(self.search_input, stretch=1)  # Input expands to fill all remaining horizontal space

        # Filter bar
        self.type_combo = QComboBox()  # Dropdown for filtering by file type category
        self.type_combo.addItems(["הכל", "תמונות", "מסמכים", "וידאו", "מוזיקה", "אחר"])  # Add all category options including "Other"
        self.type_combo.currentIndexChanged.connect(self._apply_filters)  # Re-filter instantly whenever the selection changes

        self.min_size_input = QLineEdit()  # Text input for the minimum file size in KB
        self.min_size_input.setPlaceholderText("0")  # Placeholder makes the expected input format clear
        self.min_size_input.setFixedWidth(80)  # Narrow fixed width since only a number is expected
        self.min_size_input.setValidator(QIntValidator(0, 10_000_000))  # Reject non-integer input; cap at 10 GB expressed in KB
        self.min_size_input.textChanged.connect(self._apply_filters)  # Re-filter instantly as the user types

        self.date_combo = QComboBox()  # Dropdown for filtering by modification date range
        self.date_combo.addItems(["הכל", "היום", "השבוע", "החודש", "השנה"])  # Add all date range options
        self.date_combo.currentIndexChanged.connect(self._apply_filters)  # Re-filter instantly whenever the selection changes

        filter_bar = QHBoxLayout()  # Horizontal bar that holds all three filter controls
        filter_bar.addWidget(QLabel("סוג:"))  # Label for the type dropdown
        filter_bar.addWidget(self.type_combo)  # Type category dropdown
        filter_bar.addSpacing(12)  # Visual gap between filter groups
        filter_bar.addWidget(QLabel("גודל מינימלי (KB):"))  # Label for the size input
        filter_bar.addWidget(self.min_size_input)  # Minimum size input field
        filter_bar.addSpacing(12)  # Visual gap between filter groups
        filter_bar.addWidget(QLabel("תאריך:"))  # Label for the date dropdown
        filter_bar.addWidget(self.date_combo)  # Date range dropdown
        filter_bar.addStretch()  # Push all controls to the right, leaving empty space on the left

        scan_tab_widget = QWidget()  # Container widget that holds the filter bar and scan table for tab 1
        scan_tab_layout = QVBoxLayout(scan_tab_widget)  # Vertical layout stacks the filter bar above the table
        scan_tab_layout.setContentsMargins(0, 4, 0, 0)  # Small top margin so the search bar doesn't touch the tab edge
        scan_tab_layout.setSpacing(4)  # Small gap between each row in the tab
        scan_tab_layout.addLayout(search_row)  # Search bar sits at the very top of the tab
        scan_tab_layout.addLayout(filter_bar)  # Filter bar sits directly below the search bar
        scan_tab_layout.addWidget(self.table, stretch=1)  # Table expands to fill all remaining vertical space

        self.tabs.addTab(scan_tab_widget, "סריקה")  # Add the container as the first tab labelled "Scan"

        # Tab 2 — Duplicates
        self.dup_table = _make_table(  # Build the duplicates table using the shared helper
            ["", "שם", "גודל", "תיקייה", "קבוצה"],  # Col 0 is the checkbox (no header text); then Name, Size, Folder, Group
            stretch_col=3,  # Stretch the Folder column (now col 3) to fill remaining width
        )
        self.dup_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)  # Disable row selection entirely so the blue highlight indicators don't appear
        self.dup_table.cellDoubleClicked.connect(self._on_dup_row_double_clicked)  # Open the file when the user double-clicks any cell in the duplicates table

        self.select_dups_btn = QPushButton("בחר כפולים")  # Button that auto-checks all but the first file in every duplicate group
        self.select_dups_btn.setEnabled(False)  # Disabled until the duplicates table is populated with results
        self.select_dups_btn.clicked.connect(self._select_duplicates)  # Connect click to the auto-select handler

        self.deselect_btn = QPushButton("בטל בחירה")  # Button that unchecks every checkbox in the duplicates table at once
        self.deselect_btn.setEnabled(False)  # Disabled until the duplicates table is populated with results
        self.deselect_btn.clicked.connect(self._deselect_all)  # Connect click to the deselect handler

        self.delete_btn = QPushButton("מחק נבחרים")  # Button that sends all checked files to the Recycle Bin
        self.delete_btn.setEnabled(False)  # Disabled until the duplicates table is populated with results
        self.delete_btn.clicked.connect(self._delete_selected)  # Connect click to the deletion handler

        dup_bottom_bar = QHBoxLayout()  # Horizontal layout so all buttons sit side by side at the bottom of the tab
        dup_bottom_bar.addWidget(self.select_dups_btn)  # "Select duplicates" button on the right side (RTL)
        dup_bottom_bar.addWidget(self.deselect_btn)  # "Deselect all" button next to it
        dup_bottom_bar.addWidget(self.delete_btn)  # "Delete selected" button next to it
        dup_bottom_bar.addStretch()  # Push all buttons to the right, leaving empty space on the left

        dup_tab_widget = QWidget()  # Container widget that holds the table and the button bar for tab 2
        dup_tab_layout = QVBoxLayout(dup_tab_widget)  # Vertical layout stacks the table above the button bar
        dup_tab_layout.setContentsMargins(0, 0, 0, 0)  # No extra padding inside the tab container
        dup_tab_layout.setSpacing(4)  # Small gap between table and button bar
        dup_tab_layout.addWidget(self.dup_table, stretch=1)  # Table expands to fill all available vertical space
        dup_tab_layout.addLayout(dup_bottom_bar)  # Button bar sits flush at the bottom of the tab

        self.tabs.addTab(dup_tab_widget, "כפילויות")  # Add the container as the second tab labelled "Duplicates"

        # Tab 3 — Storage Cleanup
        # --- Root picker section ---
        self.cleanup_roots_list = QListWidget()  # Displays the list of root folders the user has added for cleanup scanning
        self.cleanup_roots_list.setFixedHeight(100)  # Keep the list compact so it doesn't crowd the sections below it
        self.cleanup_roots_list.itemSelectionChanged.connect(self._on_cleanup_selection_changed)  # Enable/disable the Remove button based on whether an item is selected

        self.cleanup_add_btn = QPushButton("הוסף תיקייה")  # Opens a folder picker dialog and appends the chosen path to the list
        self.cleanup_add_btn.setFixedWidth(120)  # Consistent button width
        self.cleanup_add_btn.clicked.connect(self._on_cleanup_add_root)  # Connect to the add handler

        self.cleanup_remove_btn = QPushButton("הסר")  # Removes the currently selected path from the list
        self.cleanup_remove_btn.setFixedWidth(80)  # Consistent button width
        self.cleanup_remove_btn.setEnabled(False)  # Disabled until the user selects an item in the list
        self.cleanup_remove_btn.clicked.connect(self._on_cleanup_remove_root)  # Connect to the remove handler

        self.cleanup_scan_btn = QPushButton("סרוק")  # Starts the CleanupWorker once at least one root has been added
        self.cleanup_scan_btn.setFixedWidth(80)  # Consistent button width
        self.cleanup_scan_btn.setEnabled(False)  # Disabled until at least one root is present in the list
        self.cleanup_scan_btn.clicked.connect(self._start_cleanup_scan)  # Wire click to the cleanup scan handler

        roots_btn_row = QHBoxLayout()  # Horizontal row that holds the three action buttons for the root picker
        roots_btn_row.addWidget(self.cleanup_add_btn)   # "Add folder" on the right (RTL)
        roots_btn_row.addWidget(self.cleanup_remove_btn)  # "Remove" next to it
        roots_btn_row.addStretch()  # Push buttons right, leaving space on the left
        roots_btn_row.addWidget(self.cleanup_scan_btn)  # "Scan" pinned to the left edge (RTL: right in layout)

        roots_section = QVBoxLayout()  # Vertical stack for the root-picker header label, list, and buttons
        roots_section.setSpacing(4)  # Compact spacing between label, list, and buttons
        roots_section.addWidget(QLabel("תיקיות לסריקה:"))  # Section label: "Folders to scan:"
        roots_section.addWidget(self.cleanup_roots_list)  # The list of root paths
        roots_section.addLayout(roots_btn_row)  # Action buttons below the list

        cleanup_tab_widget = QWidget()  # Container widget for the entire cleanup tab
        cleanup_tab_layout = QVBoxLayout(cleanup_tab_widget)  # Vertical layout stacks the root picker above future sections
        cleanup_tab_layout.setContentsMargins(0, 4, 0, 0)  # Small top margin so content doesn't touch the tab edge
        cleanup_tab_layout.setSpacing(8)  # Gap between sections
        cleanup_tab_layout.addLayout(roots_section)  # Root picker stays fixed above the scroll area so it is always visible

        # Scroll content widget — wraps all three result sections so the tab is usable at any window height
        scroll_content = QWidget()  # Content widget whose layout holds Large Files, Old Files, and Heavy Folders
        scroll_content_layout = QVBoxLayout(scroll_content)  # Vertical stack for the three result sections
        scroll_content_layout.setContentsMargins(0, 4, 4, 4)  # Small right/bottom margins so content doesn't crowd the scrollbar
        scroll_content_layout.setSpacing(16)  # Generous gap between sections so they read as visually distinct groups

        scroll_area = QScrollArea()  # Scrollable viewport that gives each section room regardless of window height
        scroll_area.setWidget(scroll_content)  # Attach the content widget to the viewport
        scroll_area.setWidgetResizable(True)  # CRITICAL: allows the content widget to resize horizontally with the tab width
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)  # Only vertical scrolling is needed here

        cleanup_tab_layout.addWidget(scroll_area, stretch=1)  # Scroll area expands to fill all space below the root picker

        # --- Large Files section ---
        self.large_size_spin = QSpinBox()  # Threshold: only files at or above this size (in MB) are shown as large
        self.large_size_spin.setRange(1, 100_000)  # 1 MB to ~100 GB expressed in megabytes
        self.large_size_spin.setValue(100)  # Sensible default: flag files of 100 MB or more
        self.large_size_spin.setSuffix(" MB")  # Unit label appended to the displayed number

        self.large_files_table = _make_table(  # Checkbox table that lists files exceeding the size threshold
            ["", "שם", "גודל", "תיקייה"],  # Col 0 = checkbox; then Name, Size, Folder
            stretch_col=3,  # Stretch the Folder column to fill remaining width
        )
        self.large_files_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)  # No blue row highlight — checkboxes are the selection mechanism
        self.large_files_table.setMinimumHeight(150)  # Ensure the table is usable even when no results have been loaded yet

        self.large_files_label = QLabel("0 קבצים גדולים | 0 MB")  # Summary line updated by _on_cleanup_done in Task 8
        self.large_files_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align for RTL consistency

        self.large_delete_btn = QPushButton("מחק נבחרים")  # Sends checked large files to the Recycle Bin
        self.large_delete_btn.setEnabled(False)  # Disabled until Task 8 populates the table with results
        self.large_delete_btn.clicked.connect(
            lambda: self._delete_checked_rows(self.large_files_table, name_col=1, folder_col=3)
        )

        large_header = QHBoxLayout()  # Header row: section title on the right, threshold control on the left (RTL)
        large_header.addWidget(QLabel("קבצים גדולים"))  # Section title
        large_header.addStretch()  # Push the threshold control to the opposite end
        large_header.addWidget(QLabel("גודל מינימלי:"))  # Spinbox label
        large_header.addWidget(self.large_size_spin)  # The threshold spinbox

        large_footer = QHBoxLayout()  # Footer row: summary label on the right, delete button on the left (RTL)
        large_footer.addWidget(self.large_files_label)  # Summary: "X קבצים גדולים | Y MB"
        large_footer.addStretch()
        large_footer.addWidget(self.large_delete_btn)

        large_section = QVBoxLayout()  # Stacks header → table → footer vertically
        large_section.setSpacing(4)
        large_section.addLayout(large_header)
        large_section.addWidget(self.large_files_table, stretch=1)  # Table expands to fill all available space in this section
        large_section.addLayout(large_footer)

        scroll_content_layout.addLayout(large_section)  # Large Files section — first in the scrollable area

        # --- Old Files section ---
        self.old_months_spin = QSpinBox()  # Threshold: files not modified within this many months are considered old
        self.old_months_spin.setRange(1, 120)  # 1 month to 10 years
        self.old_months_spin.setValue(6)  # Sensible default: flag files untouched for 6 months or more
        self.old_months_spin.setSuffix(" חודשים")  # Unit label: "months"

        self.old_files_table = _make_table(  # Checkbox table that lists files older than the threshold
            ["", "שם", "גודל", "תאריך שינוי", "תיקייה"],  # Col 0 = checkbox; then Name, Size, Modified Date, Folder
            stretch_col=4,  # Stretch the Folder column to fill remaining width
        )
        self.old_files_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)  # No blue row highlight — checkboxes are the selection mechanism
        self.old_files_table.setMinimumHeight(150)  # Ensure the table is usable even when no results have been loaded yet

        self.old_files_label = QLabel("0 קבצים ישנים | 0 MB")  # Summary line updated by _on_cleanup_done in Task 8
        self.old_files_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align for RTL consistency

        self.old_delete_btn = QPushButton("מחק נבחרים")  # Sends checked old files to the Recycle Bin
        self.old_delete_btn.setEnabled(False)  # Disabled until Task 8 populates the table with results
        self.old_delete_btn.clicked.connect(
            lambda: self._delete_checked_rows(self.old_files_table, name_col=1, folder_col=4)
        )

        old_header = QHBoxLayout()  # Header row: section title on the right, threshold control on the left (RTL)
        old_header.addWidget(QLabel("קבצים ישנים"))  # Section title
        old_header.addStretch()  # Push the threshold control to the opposite end
        old_header.addWidget(QLabel("לא שונה מזה:"))  # Spinbox label: "Not modified for:"
        old_header.addWidget(self.old_months_spin)  # The threshold spinbox

        old_footer = QHBoxLayout()  # Footer row: summary label on the right, delete button on the left (RTL)
        old_footer.addWidget(self.old_files_label)  # Summary: "X קבצים ישנים | Y MB"
        old_footer.addStretch()
        old_footer.addWidget(self.old_delete_btn)

        old_section = QVBoxLayout()  # Stacks header → table → footer vertically
        old_section.setSpacing(4)
        old_section.addLayout(old_header)
        old_section.addWidget(self.old_files_table, stretch=1)  # Table expands to fill all available space in this section
        old_section.addLayout(old_footer)

        scroll_content_layout.addLayout(old_section)  # Old Files section — second in the scrollable area

        # --- Heavy Folders section ---
        # Display-only — shows the 10 immediate-parent folders with the highest combined file size.
        # No threshold spinbox and no delete button: this section is informational only.
        self.heavy_folders_table = _make_table(  # Read-only table listing the top-10 heaviest folders
            ["תיקייה", "גודל כולל", "קבצים"],  # Folder path, combined size of direct children, file count
            stretch_col=0,  # Stretch the Folder column; Size and Count columns fit their content
        )
        self.heavy_folders_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)  # Display-only — nothing to act on
        self.heavy_folders_table.setMinimumHeight(150)  # Ensure the table is usable even when no results have been loaded yet

        heavy_header = QHBoxLayout()  # Header row: section title on the right (RTL)
        heavy_header.addWidget(QLabel("תיקיות כבדות"))  # Section title: "Heavy Folders"
        heavy_header.addStretch()  # Remaining space left empty — no threshold control needed for this section

        heavy_section = QVBoxLayout()  # Stacks header → table vertically
        heavy_section.setSpacing(4)
        heavy_section.addLayout(heavy_header)
        heavy_section.addWidget(self.heavy_folders_table, stretch=1)  # Table expands to fill all available space in this section

        scroll_content_layout.addLayout(heavy_section)  # Heavy Folders section — third in the scrollable area

        # --- Duplicate Files section ---
        self.cleanup_dup_table = _make_table(  # Checkbox table listing duplicate file copies found during the hash pass
            ["", "שם", "גודל", "תיקייה"],  # Col 0 = checkbox; then Name, Size, Folder — same shape as large/old tables
            stretch_col=3,  # Stretch the Folder column to fill remaining width
        )
        self.cleanup_dup_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)  # No blue row highlight — checkboxes are the selection mechanism
        self.cleanup_dup_table.setMinimumHeight(150)  # Ensure the table is usable even when no results have been loaded yet

        self.cleanup_dup_label = QLabel("0 קבוצות כפולות | ניתן לפנות: 0 MB")  # Summary updated by _populate_cleanup_duplicates
        self.cleanup_dup_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align for RTL consistency

        self.cleanup_dup_delete_btn = QPushButton("מחק נבחרים")  # Sends checked duplicate copies to the Recycle Bin
        self.cleanup_dup_delete_btn.setEnabled(False)  # Disabled until _populate_cleanup_duplicates finds at least one group
        self.cleanup_dup_delete_btn.clicked.connect(
            lambda: self._delete_checked_rows(self.cleanup_dup_table, name_col=1, folder_col=3)
        )

        dup_cleanup_header = QHBoxLayout()  # Header row: section title on the right (RTL); no threshold control needed
        dup_cleanup_header.addWidget(QLabel("קבצים כפולים"))  # Section title: "Duplicate Files"
        dup_cleanup_header.addStretch()

        dup_cleanup_footer = QHBoxLayout()  # Footer row: summary label on the right, delete button on the left (RTL)
        dup_cleanup_footer.addWidget(self.cleanup_dup_label)
        dup_cleanup_footer.addStretch()
        dup_cleanup_footer.addWidget(self.cleanup_dup_delete_btn)

        dup_cleanup_section = QVBoxLayout()  # Stacks header → table → footer vertically
        dup_cleanup_section.setSpacing(4)
        dup_cleanup_section.addLayout(dup_cleanup_header)
        dup_cleanup_section.addWidget(self.cleanup_dup_table, stretch=1)  # Table expands to fill all available space in this section
        dup_cleanup_section.addLayout(dup_cleanup_footer)

        scroll_content_layout.addLayout(dup_cleanup_section)  # Duplicate Files section — last in the scrollable area
        scroll_content_layout.addStretch()  # Push all four sections to the top when the content is shorter than the viewport

        self.tabs.addTab(cleanup_tab_widget, "ניקוי אחסון")  # Add as the third tab labelled "Storage Cleanup"

        main_layout.addWidget(self.tabs, stretch=1)  # Add the tab widget and let it expand to fill available vertical space

        # --- Bottom Status Bar ---
        self.status_label = QLabel("0 קבצים | 0 MB")  # Status bar label; starts with zero counts
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignRight)  # Right-align for RTL consistency

        main_layout.addWidget(self.status_label)  # Add the status label at the bottom of the layout

    # --- Scan flow ---

    def choose_folder(self):  # Called when the user clicks "בחר תיקייה"
        folder_path = QFileDialog.getExistingDirectory(  # Open a native OS folder picker dialog
            self,  # Parent widget centers the dialog over the main window
            "בחר תיקייה",  # Dialog title in Hebrew
            "",  # Empty string lets the OS pick the starting directory
        )

        if not folder_path:  # User cancelled — folder_path is an empty string
            return  # Do nothing and return early

        self.path_label.setText(folder_path)  # Show the chosen path in the label

        self.select_btn.setEnabled(False)  # Disable the button to prevent a second scan while one is already running
        self.dup_btn.setEnabled(False)  # Also disable duplicates button until the new scan finishes
        self.progress_bar.setMaximum(0)  # Reset to indeterminate mode until the worker emits the real total
        self.progress_bar.setValue(0)  # Reset the fill to empty before the new scan starts
        self.progress_bar.show()  # Reveal the progress bar while scanning is in progress
        self.status_label.setText("סורק...")  # Show "Scanning…" so the user knows work is underway

        self.scan_worker = ScanWorker(folder_path)  # Store as self.scan_worker — CRITICAL: a local variable would be garbage-collected immediately
        self.scan_worker.total_files.connect(self.progress_bar.setMaximum)  # Wire total count → setMaximum so the bar switches from indeterminate to percentage mode
        self.scan_worker.progress.connect(self.progress_bar.setValue)       # Wire per-file count → setValue so the bar fills as files are found
        self.scan_worker.progress.connect(self._on_scan_progress)           # Also update the status label text with the live count
        self.scan_worker.finished.connect(self._on_scan_done)               # Connect finished signal BEFORE start()
        self.scan_worker.error.connect(self._on_scan_error)                 # Connect error signal BEFORE start()
        self.scan_worker.start()                                             # Launch the background thread; run() executes on the worker thread

    def _on_scan_progress(self, count: int):  # Called on the main thread each time the worker finds another file
        self.status_label.setText(f"סורק... {count} קבצים")  # Update the status label with the live file count

    def _on_scan_done(self, files: list[dict]):  # Called on the main thread when the worker emits finished — safe to update UI here
        self._scanned_files = files  # Cache the results for the duplicates worker to reuse later
        self.progress_bar.hide()  # Hide the progress bar now that scanning is complete
        self.select_btn.setEnabled(True)  # Re-enable the folder button so the user can scan a different folder
        self.dup_btn.setEnabled(True)  # Enable the duplicates button now that we have data to search
        self._populate_scan_table(files)  # Fill tab 1 with the scan results
        self.tabs.setCurrentIndex(0)  # Switch to the scan tab so the user sees results immediately

    def _on_scan_error(self, message: str):  # Called on the main thread when the scan worker emits error
        self.progress_bar.hide()  # Hide the progress bar since the operation ended (with failure)
        self.select_btn.setEnabled(True)  # Re-enable the button so the user can try again
        self.status_label.setText(f"שגיאה: {message}")  # Display the error in the status bar

    # --- Duplicates flow ---

    def find_dups(self):  # Called when the user clicks "מצא כפילויות"
        self.dup_btn.setEnabled(False)  # Disable the button to prevent starting a second search while one is running
        self.select_btn.setEnabled(False)  # Disable the scan button too so state cannot change mid-search
        self.progress_bar.setMaximum(0)  # Reset to indeterminate mode until the worker emits the real total
        self.progress_bar.setValue(0)  # Reset the fill to empty before the new search starts
        self.progress_bar.show()  # Show the progress bar while hashing runs in the background
        self.status_label.setText("מחפש כפילויות...")  # Show a "searching…" message in the status bar

        self.dup_worker = DuplicatesWorker(self._scanned_files)  # Store as self.dup_worker — CRITICAL: a local variable would be garbage-collected immediately
        self.dup_worker.total_files.connect(self.progress_bar.setMaximum)  # Wire total count → setMaximum so the bar switches from indeterminate to percentage mode
        self.dup_worker.progress.connect(self.progress_bar.setValue)       # Wire per-file count → setValue so the bar fills as files are hashed
        self.dup_worker.finished.connect(self._on_dups_done)               # Connect finished signal BEFORE start() so no result can be missed
        self.dup_worker.error.connect(self._on_dups_error)                 # Connect error signal BEFORE start() for the same reason
        self.dup_worker.start()                                             # Launch the background thread; run() executes on the worker thread

    def _on_dups_done(self, duplicates: dict):  # Called on the main thread when the duplicates worker emits finished — safe to update UI here
        self.progress_bar.hide()  # Hide the progress bar now that hashing is complete
        self.dup_btn.setEnabled(True)  # Re-enable the duplicates button
        self.select_btn.setEnabled(True)  # Re-enable the scan button
        self._populate_dup_table(duplicates)  # Fill tab 2 with the duplicate results
        self.tabs.setCurrentIndex(1)  # Switch to the duplicates tab so the user sees results immediately

    def _on_dups_error(self, message: str):  # Called on the main thread when the duplicates worker emits error
        self.progress_bar.hide()  # Hide the progress bar since the operation ended (with failure)
        self.dup_btn.setEnabled(True)  # Re-enable so the user can try again
        self.select_btn.setEnabled(True)  # Re-enable the scan button as well
        self.status_label.setText(f"שגיאה: {message}")  # Display the error in the status bar

    # --- Cleanup root picker ---

    def _on_cleanup_add_root(self):  # Called when the user clicks "הוסף תיקייה"
        path = QFileDialog.getExistingDirectory(  # Open a native folder picker dialog
            self,  # Parent widget centers the dialog over the main window
            "בחר תיקייה לסריקה",  # Dialog title: "Select folder to scan"
            "",  # Let the OS choose the starting directory
        )
        if not path:  # User cancelled — path is an empty string
            return

        existing = [self.cleanup_roots_list.item(i).text()  # Build a list of already-added paths
                    for i in range(self.cleanup_roots_list.count())]
        if path in existing:  # Skip duplicates — the same root cannot appear twice
            return

        self.cleanup_roots_list.addItem(path)  # Append the new path to the visible list
        self._on_cleanup_roots_changed()  # Update the scan button state

    def _on_cleanup_remove_root(self):  # Called when the user clicks "הסר"
        selected = self.cleanup_roots_list.selectedItems()  # Get the currently highlighted items
        for item in selected:  # Remove each selected item (usually just one)
            self.cleanup_roots_list.takeItem(self.cleanup_roots_list.row(item))  # Remove by row index
        self._on_cleanup_roots_changed()  # Update the scan button state

    def _on_cleanup_selection_changed(self):  # Called whenever the selection in the roots list changes
        has_selection = bool(self.cleanup_roots_list.selectedItems())  # True if at least one item is highlighted
        self.cleanup_remove_btn.setEnabled(has_selection)  # Enable Remove only when something is selected

    def _on_cleanup_roots_changed(self):  # Updates button states whenever the roots list contents change
        has_roots = self.cleanup_roots_list.count() > 0  # True if at least one root has been added
        self.cleanup_scan_btn.setEnabled(has_roots)  # Enable Scan only when there is something to scan

    # --- Cleanup scan flow ---

    def _start_cleanup_scan(self):  # Called when the user clicks "סרוק" in the cleanup tab
        roots = [
            self.cleanup_roots_list.item(i).text()  # Read each root path from the list widget by index
            for i in range(self.cleanup_roots_list.count())
        ]

        self.cleanup_scan_btn.setEnabled(False)   # Prevent a second scan from starting while one is already running
        self.cleanup_add_btn.setEnabled(False)    # Disable Add so the root list cannot change mid-scan
        self.cleanup_remove_btn.setEnabled(False) # Disable Remove for the same reason

        self.progress_bar.setMaximum(0)   # Reset to indeterminate (pulsing) mode until the worker emits the real total
        self.progress_bar.setValue(0)     # Clear the fill before the new scan starts
        self.progress_bar.show()          # Reveal the progress bar while scanning is in progress
        self.status_label.setText("סורק לניקוי...")  # "Scanning for cleanup…"

        self.cleanup_worker = CleanupWorker(roots)  # Store as self.cleanup_worker — CRITICAL: a local variable would be garbage-collected immediately
        self.cleanup_worker.total_files.connect(self.progress_bar.setMaximum)      # Switch bar from indeterminate to percentage mode when the scan total arrives
        self.cleanup_worker.progress.connect(self.progress_bar.setValue)         # Advance the bar one step per file during the scan pass
        self.cleanup_worker.progress.connect(self._on_cleanup_scan_progress)     # Update the status label with the live scan count
        self.cleanup_worker.hash_start.connect(self._on_cleanup_hash_start)      # Reset bar and status label when the hash pass begins
        self.cleanup_worker.hash_progress.connect(self.progress_bar.setValue)    # Advance the bar one step per file during the hash pass — no status label side-effect
        self.cleanup_worker.finished.connect(self._on_cleanup_done)              # Connect finished signal BEFORE start()
        self.cleanup_worker.error.connect(self._on_cleanup_error)                # Connect error signal BEFORE start()
        self.cleanup_worker.start()                                               # Launch the background thread

    def _on_cleanup_scan_progress(self, count: int):  # Called on the main thread each time the worker finds another file during the scan pass
        self.status_label.setText(f"סורק לניקוי... {count} קבצים")  # Live count so the user sees progress during long scans

    def _on_cleanup_hash_start(self, total: int):  # Called on the main thread when the worker transitions from scanning to hashing
        self.progress_bar.setMaximum(total)  # Reset the bar maximum to the number of files about to be hashed
        self.progress_bar.setValue(0)        # Reset the fill so the bar sweeps from 0 % to 100 % again during hashing
        self.status_label.setText("מחפש כפילויות...")  # "Finding duplicates…" — stays fixed while hash_progress silently drives the bar

    def _on_cleanup_done(self, files: list[dict], duplicates: dict):  # Called on the main thread when CleanupWorker emits finished(files, duplicates) — safe to update UI here
        self._cleanup_files = files  # Cache the file list in case the user re-runs with changed thresholds
        self.progress_bar.hide()  # Hide the progress bar now that both scan and hash passes are complete
        self.cleanup_scan_btn.setEnabled(True)   # Re-enable so the user can re-scan after changing roots or thresholds
        self.cleanup_add_btn.setEnabled(True)    # Re-enable the Add button
        self._on_cleanup_roots_changed()         # Re-evaluate Remove/Scan enabled states based on current list contents
        self._populate_large_files(files)        # Fill the Large Files section with files that exceed the spinbox threshold
        self._populate_old_files(files)          # Fill the Old Files section with files older than the spinbox threshold
        self._populate_heavy_folders(files)      # Fill the Heavy Folders section with the top-10 heaviest immediate-parent folders
        self._populate_cleanup_duplicates(duplicates)  # Fill the Duplicate Files section with the hashing results from the worker
        self.status_label.setText(f"סיום סריקה — {len(files)} קבצים")  # "Scan complete — X files"

    def _on_cleanup_error(self, message: str):  # Called on the main thread when CleanupWorker emits error
        self.progress_bar.hide()  # Hide the progress bar since the operation ended with failure
        self.cleanup_scan_btn.setEnabled(True)   # Re-enable so the user can try again
        self.cleanup_add_btn.setEnabled(True)    # Re-enable the Add button
        self._on_cleanup_roots_changed()         # Re-evaluate Remove button state
        self.status_label.setText(f"שגיאה בסריקה: {message}")  # Display the error so the user knows what went wrong

    # --- File opening ---

    def _open_file(self, folder: str, name: str):  # Build the full path and open the file with its default application
        full_path = os.path.join(folder, name)  # Combine folder and name into an absolute path
        try:
            os.startfile(full_path)  # Ask Windows to open the file with whatever app is registered for its type
        except OSError as e:  # Catch errors such as file not found or no associated application
            self.status_label.setText(f"שגיאה בפתיחת קובץ: {e}")  # Show the error in the status bar so the user knows what went wrong

    def _on_scan_row_double_clicked(self, row: int, _col: int):  # Called when the user double-clicks a row in the scan table
        name   = self.table.item(row, 0).text()  # Column 0 holds the file name
        folder = self.table.item(row, 4).text()  # Column 4 holds the parent folder path
        self._open_file(folder, name)  # Delegate to the shared open helper

    def _on_dup_row_double_clicked(self, row: int, _col: int):  # Called when the user double-clicks a row in the duplicates table
        name   = self.dup_table.item(row, 1).text()  # Column 1 holds the file name (col 0 is now the checkbox)
        folder = self.dup_table.item(row, 3).text()  # Column 3 holds the parent folder path (shifted by checkbox col)
        self._open_file(folder, name)  # Delegate to the shared open helper

    def _deselect_all(self):  # Called when the user clicks "בטל בחירה"
        for row in range(self.dup_table.rowCount()):  # Iterate every row in the duplicates table
            self.dup_table.item(row, 0).setCheckState(Qt.CheckState.Unchecked)  # Uncheck the checkbox in column 0

    def _select_duplicates(self):  # Called when the user clicks "בחר כפולים"
        seen_groups: set[str] = set()  # Tracks which group numbers have already had their first row encountered

        for row in range(self.dup_table.rowCount()):  # Iterate every row top to bottom
            group = self.dup_table.item(row, 4).text()  # Group number is stored as text in column 4
            chk_item = self.dup_table.item(row, 0)  # Checkbox item is in column 0

            if group not in seen_groups:  # This is the first row encountered for this group — keep it
                chk_item.setCheckState(Qt.CheckState.Unchecked)  # Uncheck so the user retains one copy
                seen_groups.add(group)  # Mark this group as having its keeper row assigned
            else:  # This is a subsequent duplicate in the same group — mark it for deletion
                chk_item.setCheckState(Qt.CheckState.Checked)  # Check so the user can delete it in one click

    def _delete_selected(self):  # Called when the user clicks "מחק נבחרים"
        to_delete = []  # List of (row_index, full_path, size_bytes) for every checked row

        for row in range(self.dup_table.rowCount()):  # Iterate every row in the duplicates table
            chk_item = self.dup_table.item(row, 0)  # Column 0 holds the checkbox item
            if chk_item and chk_item.checkState() == Qt.CheckState.Checked:  # Only process rows where the checkbox is ticked
                name       = self.dup_table.item(row, 1).text()  # File name from column 1
                folder     = self.dup_table.item(row, 3).text()  # Parent folder from column 3
                size_bytes = chk_item.data(Qt.ItemDataRole.UserRole)  # Raw byte count stored at population time
                full_path  = os.path.join(folder, name)  # Reconstruct the absolute path
                to_delete.append((row, full_path, size_bytes))  # Collect for confirmation and deletion

        if not to_delete:  # Nothing was checked — tell the user and bail out
            self.status_label.setText("לא נבחרו קבצים למחיקה")  # "No files selected for deletion"
            return

        total_bytes = sum(size for _, _, size in to_delete)  # Sum of all selected file sizes for the confirmation message
        confirm = QMessageBox.question(  # Show a native Yes/No confirmation dialog before touching any files
            self,  # Parent widget centers the dialog over the main window
            "אישור מחיקה",  # Dialog title: "Confirm deletion"
            f"האם להעביר לאשפה {len(to_delete)} קבצים ({format_total_size(total_bytes)})?",  # "Move X files (Y MB) to Recycle Bin?"
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,  # Show Yes and No buttons
            QMessageBox.StandardButton.No,  # Default to No so an accidental Enter press does not delete anything
        )

        if confirm != QMessageBox.StandardButton.Yes:  # User clicked No or closed the dialog
            return  # Abort without deleting anything

        failed = []  # Track files that could not be trashed so we can report them

        for _row, full_path, _size in to_delete:  # Attempt to trash each selected file
            try:
                send2trash(full_path)  # Move the file to the Recycle Bin; recoverable if the user made a mistake
            except Exception as e:  # Catch permission errors or missing files
                failed.append(f"{full_path}: {e}")  # Record the failure message for the status bar

        # Remove successfully deleted rows from the table (iterate in reverse so indices stay valid as rows are removed)
        deleted_rows = {row for row, path, _ in to_delete if path not in {f for f in failed}}  # Set of rows that were actually deleted
        for row in sorted(deleted_rows, reverse=True):  # Remove from bottom to top to preserve correct indices
            self.dup_table.removeRow(row)  # Delete the row from the visible table

        remaining = self.dup_table.rowCount()  # How many rows are left after deletion
        saved_bytes = sum(size for _, _, size in to_delete) - sum(  # Recalculate savings excluding any failures
            size for _, path, size in to_delete if path in {f for f in failed}
        )

        if failed:  # At least one file could not be trashed — show partial-failure message
            self.status_label.setText(f"נמחקו חלקית — {len(failed)} שגיאות | {remaining} שורות נותרו")
        else:  # All deletions succeeded
            self.status_label.setText(  # Update status bar with deletion summary
                f"הועברו לאשפה {len(to_delete)} קבצים | חסכון: {format_total_size(saved_bytes)}"  # "X files moved to Recycle Bin | Saved: Y MB"
            )

    def _delete_checked_rows(self, table: QTableWidget, name_col: int, folder_col: int):  # Shared deletion handler for Large Files and Old Files tables
        to_delete = []  # Collect (row_index, full_path, size_bytes) for every checked row before touching anything

        for row in range(table.rowCount()):
            chk_item = table.item(row, 0)  # Checkbox is always in column 0 for both cleanup tables
            if chk_item and chk_item.checkState() == Qt.CheckState.Checked:
                name       = table.item(row, name_col).text()    # File name from the caller-supplied column
                folder     = table.item(row, folder_col).text()  # Parent folder from the caller-supplied column
                size_bytes = chk_item.data(Qt.ItemDataRole.UserRole)  # Raw bytes stored at population time — no string parsing needed
                full_path  = os.path.join(folder, name)
                to_delete.append((row, full_path, size_bytes))

        if not to_delete:
            self.status_label.setText("לא נבחרו קבצים למחיקה")  # "No files selected for deletion"
            return

        total_bytes = sum(size for _, _, size in to_delete)
        confirm = QMessageBox.question(
            self,
            "אישור מחיקה",  # "Confirm deletion"
            f"האם להעביר לאשפה {len(to_delete)} קבצים ({format_total_size(total_bytes)})?",  # "Move X files (Y) to Recycle Bin?"
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,  # Default to No so an accidental Enter press does not delete anything
        )

        if confirm != QMessageBox.StandardButton.Yes:
            return

        failed_paths: set[str] = set()  # Track paths that could not be trashed so rows are not removed from the table

        for _row, full_path, _size in to_delete:
            try:
                send2trash(full_path)  # Move to Recycle Bin — recoverable if the user made a mistake
            except Exception:
                failed_paths.add(full_path)  # Record only the path; the row stays in the table so the user can retry

        # Remove rows bottom-to-top so earlier indices stay valid as rows are removed
        for row in sorted(
            (row for row, path, _ in to_delete if path not in failed_paths),
            reverse=True,
        ):
            table.removeRow(row)

        saved_bytes = sum(size for _, path, size in to_delete if path not in failed_paths)

        if failed_paths:
            self.status_label.setText(
                f"נמחקו חלקית — {len(failed_paths)} שגיאות | {len(to_delete) - len(failed_paths)} הועברו לאשפה"
            )
        else:
            self.status_label.setText(
                f"הועברו לאשפה {len(to_delete)} קבצים | חסכון: {format_total_size(saved_bytes)}"  # "X files moved to Recycle Bin | Saved: Y"
            )

    # --- Filtering ---

    def _apply_filters(self):  # Show/hide scan table rows instantly based on the current filter values; called on every filter change
        type_label  = self.type_combo.currentText()   # Currently selected type category ("הכל" means no type filter)
        size_text   = self.min_size_input.text().strip()  # Raw text from the size input; may be empty
        min_bytes   = int(size_text) * 1024 if size_text.isdigit() and int(size_text) > 0 else 0  # Convert KB to bytes; 0 means no size filter
        date_label  = self.date_combo.currentText()   # Currently selected date range ("הכל" means no date filter)
        today       = datetime.now().date()           # Today's date used as the anchor for all relative date comparisons
        search_term = self.search_input.text().lower()  # Lowercased search string; empty string means no name filter

        visible_count = 0  # Running count of rows that pass all filters
        visible_bytes = 0  # Running total bytes of visible rows for the status bar

        for row in range(self.table.rowCount()):  # Check every row in the scan table
            name_item = self.table.item(row, 0)  # Name cell holds the raw file dict in UserRole
            file = name_item.data(Qt.ItemDataRole.UserRole) if name_item else None  # Retrieve the stored dict

            if file is None:  # Row has no data (table is being rebuilt) — leave it visible
                self.table.setRowHidden(row, False)
                continue

            hide = False  # Assume visible until a filter decides otherwise

            # Type filter
            if type_label != "הכל":  # "הכל" = show all types
                ext = file["file_type"]  # Already lowercase, no leading dot
                if type_label == "אחר":  # "אחר" = extensions not belonging to any named group
                    hide = any(ext in exts for exts in FILE_TYPE_GROUPS.values())  # Hide if it IS in a known group
                else:
                    hide = ext not in FILE_TYPE_GROUPS.get(type_label, set())  # Hide if not in the selected group

            # Size filter
            if not hide and file["size_bytes"] < min_bytes:  # Hide files smaller than the minimum
                hide = True

            # Search filter
            if not hide and search_term and search_term not in file["name"].lower():  # Non-empty term that does not appear in the filename → hide
                hide = True

            # Date filter
            if not hide and date_label != "הכל":  # "הכל" = show all dates
                mod = file["modified_date"].date()  # Extract just the date part from the stored datetime object
                if date_label == "היום":
                    hide = mod != today  # Must be today
                elif date_label == "השבוע":
                    hide = (today - mod).days > 7  # Within the last 7 days
                elif date_label == "החודש":
                    hide = mod.year != today.year or mod.month != today.month  # Same calendar month and year
                elif date_label == "השנה":
                    hide = mod.year != today.year  # Same calendar year

            self.table.setRowHidden(row, hide)  # Apply the visibility decision to this row

            highlight = not hide and bool(search_term)  # Yellow background only when a search is active and this row matched it
            name_item.setBackground(  # Paint the filename cell to give the user a clear visual indicator of which rows matched
                QBrush(QColor(255, 255, 180)) if highlight else QBrush()  # Soft yellow for matches; default (null) brush resets to the normal cell colour
            )

            if not hide:  # Accumulate stats only for rows that remain visible
                visible_count += 1
                visible_bytes += file["size_bytes"]

        total = self.table.rowCount()  # Total rows regardless of visibility
        if visible_count == total:  # No rows were hidden — show the plain total
            self.status_label.setText(f"{total} קבצים | {format_total_size(visible_bytes)}")
        else:  # Some rows are filtered out — show visible vs total
            self.status_label.setText(f"מציג {visible_count} מתוך {total} קבצים | {format_total_size(visible_bytes)}")

    # --- Table population ---

    def _populate_scan_table(self, files: list[dict]):  # Fill the scan table with a list of file dicts
        self.table.setRowCount(len(files))  # Resize the table to match the number of files

        total_bytes = 0  # Accumulator for total size across all files

        for row, file in enumerate(files):  # Iterate over each file dict with its row index
            name_item = QTableWidgetItem(file["name"])  # Cell: file name
            name_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align
            name_item.setData(Qt.ItemDataRole.UserRole, file)  # Store the full file dict so _apply_filters can read raw values without parsing display strings

            size_item = QTableWidgetItem(format_size(file["size_bytes"]))  # Cell: formatted size
            size_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)  # Center-align

            type_item = QTableWidgetItem(file["file_type"] or "—")  # Cell: extension or dash if none
            type_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)  # Center-align

            date_item = QTableWidgetItem(file["modified_date"].strftime("%d/%m/%Y"))  # Cell: date formatted as DD/MM/YYYY
            date_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)  # Center-align

            folder_item = QTableWidgetItem(file["folder"])  # Cell: parent folder path
            folder_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align

            self.table.setItem(row, 0, name_item)   # Insert into column 0 (Name)
            self.table.setItem(row, 1, size_item)   # Insert into column 1 (Size)
            self.table.setItem(row, 2, type_item)   # Insert into column 2 (Type)
            self.table.setItem(row, 3, date_item)   # Insert into column 3 (Date)
            self.table.setItem(row, 4, folder_item) # Insert into column 4 (Folder)

            total_bytes += file["size_bytes"]  # Accumulate file size

        self._apply_filters()  # Apply any active filters immediately and update the status bar with the visible count

    def _populate_dup_table(self, duplicates: dict[str, list[dict]]):  # Fill the duplicates table from a hash→files mapping
        rows = [  # Flatten the groups into a list of (group_number, file_dict) tuples for easy row iteration
            (group_num, file)  # Pair each file with its 1-based group number
            for group_num, files in enumerate(duplicates.values(), start=1)  # Enumerate groups starting from 1
            for file in files  # Expand each group into individual file rows
        ]

        self.dup_table.setRowCount(len(rows))  # Resize the table to the total number of duplicate file rows

        savings_bytes = 0  # Accumulator for wasted space (all but one file per group)

        for row, (group_num, file) in enumerate(rows):  # Iterate with row index, group number, and file dict
            chk_item = QTableWidgetItem()  # Checkbox cell; no display text — the tick box is the only content
            chk_item.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)  # Make the cell checkable and interactive but not editable as text
            chk_item.setCheckState(Qt.CheckState.Unchecked)  # Start every row unticked
            chk_item.setData(Qt.ItemDataRole.UserRole, file["size_bytes"])  # Store raw bytes so _delete_selected can calculate totals without parsing formatted strings

            name_item = QTableWidgetItem(file["name"])  # Cell: file name
            name_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align

            size_item = QTableWidgetItem(format_size(file["size_bytes"]))  # Cell: formatted file size
            size_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)  # Center-align

            folder_item = QTableWidgetItem(file["folder"])  # Cell: parent folder path
            folder_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align

            group_item = QTableWidgetItem(str(group_num))  # Cell: group number so the user can see which files are paired
            group_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)  # Center-align

            self.dup_table.setItem(row, 0, chk_item)    # Insert checkbox into column 0
            self.dup_table.setItem(row, 1, name_item)   # Insert name into column 1 (shifted by checkbox col)
            self.dup_table.setItem(row, 2, size_item)   # Insert size into column 2
            self.dup_table.setItem(row, 3, folder_item) # Insert folder into column 3
            self.dup_table.setItem(row, 4, group_item)  # Insert group into column 4

        for files in duplicates.values():  # Iterate over each duplicate group to calculate savings
            group_size = files[0]["size_bytes"]  # All files in the group are identical so any one has the right size
            savings_bytes += group_size * (len(files) - 1)  # Only one copy is needed; the rest is wasted space

        dup_count = len(duplicates)  # Number of unique duplicate groups found
        self.status_label.setText(  # Update the status bar with duplicate summary
            f"{dup_count} כפילויות | {format_total_size(savings_bytes)} לחיסכון"  # "X duplicates | Y MB saveable"
        )
        self.delete_btn.setEnabled(dup_count > 0)      # Enable the delete button only if there is at least one duplicate group to act on
        self.select_dups_btn.setEnabled(dup_count > 0)  # Enable the auto-select button under the same condition
        self.deselect_btn.setEnabled(dup_count > 0)     # Enable the deselect button under the same condition

    def _populate_large_files(self, files: list[dict]):  # Fill the Large Files table with files that meet or exceed the size threshold
        threshold_bytes = self.large_size_spin.value() * 1_048_576  # Convert the spinbox MB value to bytes for comparison

        large = sorted(  # Filter and sort in one step: largest files first so the worst offenders appear at the top
            (f for f in files if f["size_bytes"] >= threshold_bytes),
            key=lambda f: f["size_bytes"],
            reverse=True,
        )

        self.large_files_table.setRowCount(len(large))  # Resize the table to match the number of qualifying files
        total_bytes = 0  # Accumulate total size for the summary label

        for row, file in enumerate(large):
            chk_item = QTableWidgetItem()  # Checkbox cell — no display text
            chk_item.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)  # Checkable but not text-editable
            chk_item.setCheckState(Qt.CheckState.Unchecked)  # Start every row unticked
            chk_item.setData(Qt.ItemDataRole.UserRole, file["size_bytes"])  # Store raw bytes for deletion total calculation

            name_item = QTableWidgetItem(file["name"])
            name_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            size_item = QTableWidgetItem(format_size(file["size_bytes"]))
            size_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            folder_item = QTableWidgetItem(file["folder"])
            folder_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            self.large_files_table.setItem(row, 0, chk_item)
            self.large_files_table.setItem(row, 1, name_item)
            self.large_files_table.setItem(row, 2, size_item)
            self.large_files_table.setItem(row, 3, folder_item)

            total_bytes += file["size_bytes"]

        count = len(large)
        self.large_files_label.setText(f"{count} קבצים גדולים | {format_total_size(total_bytes)}")  # Update the summary label
        self.large_delete_btn.setEnabled(count > 0)  # Enable the delete button only when there is something to delete

    def _populate_old_files(self, files: list[dict]):  # Fill the Old Files table with files not modified within the threshold
        months = self.old_months_spin.value()
        cutoff = datetime.now() - timedelta(days=months * 30)  # Approximate: 30 days per month (precise enough for this use case)

        old = sorted(  # Filter and sort: oldest files first so the most neglected files appear at the top
            (f for f in files if f["modified_date"] < cutoff),
            key=lambda f: f["modified_date"],
        )

        self.old_files_table.setRowCount(len(old))  # Resize the table to match the number of qualifying files
        total_bytes = 0  # Accumulate total size for the summary label

        for row, file in enumerate(old):
            chk_item = QTableWidgetItem()  # Checkbox cell — no display text
            chk_item.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)  # Checkable but not text-editable
            chk_item.setCheckState(Qt.CheckState.Unchecked)  # Start every row unticked
            chk_item.setData(Qt.ItemDataRole.UserRole, file["size_bytes"])  # Store raw bytes for deletion total calculation

            name_item = QTableWidgetItem(file["name"])
            name_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            size_item = QTableWidgetItem(format_size(file["size_bytes"]))
            size_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            date_item = QTableWidgetItem(file["modified_date"].strftime("%d/%m/%Y"))  # Show last-modified date so the user can verify why the file was flagged
            date_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            folder_item = QTableWidgetItem(file["folder"])
            folder_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            self.old_files_table.setItem(row, 0, chk_item)
            self.old_files_table.setItem(row, 1, name_item)
            self.old_files_table.setItem(row, 2, size_item)
            self.old_files_table.setItem(row, 3, date_item)
            self.old_files_table.setItem(row, 4, folder_item)

            total_bytes += file["size_bytes"]

        count = len(old)
        self.old_files_label.setText(f"{count} קבצים ישנים | {format_total_size(total_bytes)}")  # Update the summary label
        self.old_delete_btn.setEnabled(count > 0)  # Enable the delete button only when there is something to delete

    def _populate_heavy_folders(self, files: list[dict]):  # Fill the Heavy Folders table with the 10 largest immediate-parent folders
        folder_sizes: dict[str, int] = {}   # Maps each folder path to the combined byte count of its direct-child files
        folder_counts: dict[str, int] = {}  # Maps each folder path to the count of its direct-child files

        for file in files:  # Single pass: accumulate size and count per folder
            folder = file["folder"]  # Immediate parent path — no recursive aggregation, exactly as clarified in the plan
            folder_sizes[folder]  = folder_sizes.get(folder, 0)  + file["size_bytes"]
            folder_counts[folder] = folder_counts.get(folder, 0) + 1

        top10 = sorted(folder_sizes.items(), key=lambda x: x[1], reverse=True)[:10]  # Pick the 10 folders with the most bytes

        self.heavy_folders_table.setRowCount(len(top10))  # Resize the table to at most 10 rows

        for row, (folder, total_bytes) in enumerate(top10):
            folder_item = QTableWidgetItem(folder)
            folder_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            size_item = QTableWidgetItem(format_total_size(total_bytes))
            size_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            count_item = QTableWidgetItem(str(folder_counts[folder]))
            count_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            self.heavy_folders_table.setItem(row, 0, folder_item)
            self.heavy_folders_table.setItem(row, 1, size_item)
            self.heavy_folders_table.setItem(row, 2, count_item)

    def _populate_cleanup_duplicates(self, duplicates: dict[str, list[dict]]):  # Fill the Duplicate Files table from the hash→files mapping produced by CleanupWorker
        # Flatten all groups into a list of (is_keeper, file_dict) pairs.
        # Within each group, sort by filename alphabetically and keep the first copy; mark the rest for deletion.
        rows: list[tuple[bool, dict]] = []  # (keep, file) — True = Unchecked (kept copy), False = Checked (suggested for deletion)
        savings_bytes = 0  # Bytes that could be freed by deleting all non-kept copies

        for group in duplicates.values():  # Each value is a list of file dicts that share the same SHA-256 hash
            sorted_group = sorted(group, key=lambda f: f["name"].lower())  # Alphabetical by name — deterministic and easy to understand
            for i, file in enumerate(sorted_group):
                rows.append((i == 0, file))  # First entry is the keeper; all others are pre-selected for deletion
            group_size = sorted_group[0]["size_bytes"]  # All copies are byte-for-byte identical, so any one has the right size
            savings_bytes += group_size * (len(sorted_group) - 1)  # One copy kept; the rest is reclaimable space

        self.cleanup_dup_table.setRowCount(len(rows))  # Resize the table to the total number of duplicate file rows

        for row, (keep, file) in enumerate(rows):
            chk_item = QTableWidgetItem()  # Checkbox cell — no display text
            chk_item.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)  # Checkable but not text-editable
            chk_item.setCheckState(Qt.CheckState.Unchecked if keep else Qt.CheckState.Checked)  # Keeper stays unticked; surplus copies pre-ticked
            chk_item.setData(Qt.ItemDataRole.UserRole, file["size_bytes"])  # Store raw bytes so _delete_checked_rows can total them without parsing

            name_item = QTableWidgetItem(file["name"])
            name_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            size_item = QTableWidgetItem(format_size(file["size_bytes"]))
            size_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            folder_item = QTableWidgetItem(file["folder"])
            folder_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            self.cleanup_dup_table.setItem(row, 0, chk_item)
            self.cleanup_dup_table.setItem(row, 1, name_item)
            self.cleanup_dup_table.setItem(row, 2, size_item)
            self.cleanup_dup_table.setItem(row, 3, folder_item)

        group_count = len(duplicates)
        self.cleanup_dup_label.setText(
            f"{group_count} קבוצות כפולות | ניתן לפנות: {format_total_size(savings_bytes)}"  # "X duplicate groups | Reclaimable: Y MB"
        )
        self.cleanup_dup_delete_btn.setEnabled(group_count > 0)  # Enable the delete button only when there is at least one duplicate group


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

app = QApplication(sys.argv)  # Create the Qt application, passing command-line arguments
app.setLayoutDirection(Qt.LayoutDirection.RightToLeft)  # Set RTL direction globally for all widgets

window = MainWindow()  # Instantiate the main window
window.show()  # Make the window visible on screen

sys.exit(app.exec())  # Start the event loop and exit cleanly when the app closes
