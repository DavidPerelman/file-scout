import sys       # Import sys to access command-line arguments and exit functionality
import os        # Import os for os.startfile() to open files with their default application
import threading  # Import threading for Event-based stop flag coordination between the main and worker threads
from PyQt6.QtWidgets import (  # Import all needed Qt widgets
    QApplication, QMainWindow, QWidget,  # Core window and container widgets
    QVBoxLayout, QHBoxLayout,  # Vertical and horizontal layout managers
    QPushButton, QLabel,  # Button and text label widgets
    QTableWidget, QTableWidgetItem,  # Table widget and its cell item class
    QHeaderView, QFileDialog,  # Header behavior control and folder picker dialog
    QTabWidget, QProgressBar, QMessageBox,  # Tab container, progress bar, and confirmation dialog widgets
    QComboBox, QLineEdit,  # Dropdown selector and single-line text input for the filter bar
    QListWidget,  # List widget used in the cleanup tab to display the selected root paths
    QScrollArea,  # Scrollable viewport that wraps the three cleanup result sections
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal  # Import Qt namespace, thread class, and signal type
from PyQt6.QtGui import (  # Import GUI-level classes
    QIntValidator,  # Restricts a QLineEdit to integer input only
    QColor,         # Used to set the yellow highlight background on matching filename cells
    QBrush,         # Wraps QColor into a brush accepted by QTableWidgetItem.setBackground()
)
from datetime import datetime, timedelta  # Import datetime for date comparisons; timedelta for the "older than N months" cutoff
from pathlib import Path  # Import Path for the module-level _walk_clean helper used by ScanWorker
from send2trash import send2trash  # Import send2trash to move files to the Recycle Bin instead of permanently deleting them
from duplicates import find_duplicates  # Import the duplicate detection function from duplicates.py


# ---------------------------------------------------------------------------
# Background workers
# ---------------------------------------------------------------------------

class ScanWorker(QThread):  # Single unified worker: scans all roots, emits the file list immediately, then hashes for duplicates
    total_files      = pyqtSignal(int)   # Emitted once before scanning with the combined file count; switches the bar from indeterminate to percentage mode
    progress         = pyqtSignal(int)   # Emitted after each file is discovered; drives progress bar setValue + status label during the scan pass
    files_ready      = pyqtSignal(list)  # Emitted as soon as scanning finishes — Tab 1 populates immediately without waiting for hashing
    hash_start       = pyqtSignal(int)   # Emitted before hashing begins with len(files) as the new bar maximum; resets the bar for the hash pass
    hash_progress    = pyqtSignal(int)   # Emitted after each file is hashed; drives only progress bar setValue — never touches the status label
    duplicates_ready = pyqtSignal(dict)  # Emitted when hashing completes; Tab 2 duplicate section populates from this
    stopped          = pyqtSignal()      # Emitted instead of duplicates_ready when stop() is called before the run completes
    error            = pyqtSignal(str)   # Emitted with an error message string if any unrecoverable exception is raised

    def __init__(self, roots: list[str]):  # Accept the list of root paths from the shared panel
        super().__init__()
        self._roots = roots  # Never mutated after construction
        self._stop_event = threading.Event()  # Shared flag; main thread sets it via stop(), worker checks it in run()

    def stop(self):  # Called from the main thread to request cancellation; threading.Event.set() is thread-safe
        print("stop() called")
        self._stop_event.set()

    def run(self):  # Qt calls this in the background thread when start() is invoked
        try:
            # --- Pre-count pass: use _walk_clean so the count matches what the scan pass will actually visit ---
            total = sum(
                sum(1 for _ in _walk_clean(Path(root)))
                for root in self._roots
            )
            self.total_files.emit(total)  # Progress bar switches from indeterminate to percentage mode

            # --- Scan pass: build file dicts and emit files_ready as soon as the list is complete ---
            files = []
            for root in self._roots:
                for entry in _walk_clean(Path(root)):  # Skip-aware, PermissionError-safe walk
                    try:
                        stat = entry.stat()  # May raise OSError if the file vanished between discovery and stat
                    except OSError:
                        continue
                    files.append({
                        "name":          entry.name,
                        "size_bytes":    stat.st_size,
                        "file_type":     entry.suffix.lstrip(".").lower(),
                        "modified_date": datetime.fromtimestamp(stat.st_mtime),
                        "folder":        str(entry.parent),
                    })
                    self.progress.emit(len(files))
                    if self._stop_event.is_set():  # Check after every file; exits both the entry and root loops
                        print("stop flag checked — stopping scan loop")
                        self.stopped.emit()
                        return

            self.files_ready.emit(files)  # Tab 1 populates immediately — hashing has not started yet

            if self._stop_event.is_set():  # Guard the brief window between files_ready and the hash pass starting
                self.stopped.emit()
                return

            # --- Hash pass: find duplicates; Tab 2 duplicate section populates when this completes ---
            self.hash_start.emit(len(files))  # Reset the bar for the hash phase
            duplicates = find_duplicates(
                files,
                on_progress=self.hash_progress.emit,
                stop_event=self._stop_event,  # Lets find_duplicates break out early between files
            )
            if self._stop_event.is_set():  # find_duplicates returned early; emit stopped instead of duplicates_ready
                self.stopped.emit()
                return

            self.duplicates_ready.emit(duplicates)

        except Exception as e:
            self.error.emit(str(e))


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

# Directory path prefixes that ScanWorker never recurses into (case-insensitive).
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
    name_col = headers.index("שם") if "שם" in headers else -1  # Locate the name column so we can cap its width
    for i in range(len(headers)):  # Loop over every column index
        if i == stretch_col:
            mode = QHeaderView.ResizeMode.Stretch
        elif i == name_col:
            mode = QHeaderView.ResizeMode.Fixed  # Fixed so setColumnWidth caps it
        else:
            mode = QHeaderView.ResizeMode.ResizeToContents
        table.horizontalHeader().setSectionResizeMode(i, mode)  # Apply the resize mode to this column
    if name_col >= 0:
        table.setColumnWidth(name_col, 220)  # Cap name column; long filenames will be elided
    table.setTextElideMode(Qt.TextElideMode.ElideRight)  # Clip long text with … instead of overflowing
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

        central_widget = QWidget()  # Create a plain widget to serve as the central container
        self.setCentralWidget(central_widget)  # Set it as the main content area of the window

        main_layout = QVBoxLayout(central_widget)  # Create a vertical layout attached to the central widget
        main_layout.setContentsMargins(8, 8, 8, 8)  # Set 8px padding around the edges of the layout
        main_layout.setSpacing(6)  # Set 6px gap between each section in the layout

        # --- Shared Panel: multi-root picker + scan button ---
        self.roots_list = QListWidget()  # Lists all root folders the user has selected for scanning
        self.roots_list.setFixedHeight(80)  # Compact height so the list doesn't crowd the tabs below
        self.roots_list.itemSelectionChanged.connect(self._on_roots_selection_changed)  # Gate the Remove button on selection

        self.add_root_btn = QPushButton("➕ הוסף תיקייה")  # Opens a folder picker and appends the chosen path
        self.add_root_btn.setFixedWidth(120)
        self.add_root_btn.clicked.connect(self._on_add_root)

        self.remove_root_btn = QPushButton("🗑 הסר")  # Removes the selected root from the list
        self.remove_root_btn.setObjectName("neutralBtn")
        self.remove_root_btn.setFixedWidth(80)
        self.remove_root_btn.setEnabled(False)  # Disabled until the user selects an item
        self.remove_root_btn.clicked.connect(self._on_remove_root)

        self.scan_all_btn = QPushButton("🔍 סרוק הכל")  # Starts the unified ScanWorker across all roots
        self.scan_all_btn.setMinimumWidth(130)
        self.scan_all_btn.setEnabled(False)  # Disabled until at least one root is in the list
        self.scan_all_btn.clicked.connect(self._start_scan)  # Wired to the unified scan entry point added in Task 6

        self.stop_btn = QPushButton("⏹ עצור")  # Cancels the running scan; disabled until a scan starts
        self.stop_btn.setObjectName("warningBtn")
        self.stop_btn.setFixedWidth(90)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(lambda: self.worker.stop())

        self.dark_mode = False
        self.theme_btn = QPushButton("🌙")  # Toggles between light and dark theme
        self.theme_btn.setObjectName("themeBtn")
        self.theme_btn.setFixedSize(36, 36)
        self.theme_btn.clicked.connect(self._toggle_theme)

        shared_btn_row = QHBoxLayout()  # Add / Remove on the right (RTL); Scan pinned to the left
        shared_btn_row.addWidget(self.theme_btn)
        shared_btn_row.addWidget(self.add_root_btn)
        shared_btn_row.addWidget(self.remove_root_btn)
        shared_btn_row.addStretch()
        shared_btn_row.addWidget(self.stop_btn)
        shared_btn_row.addWidget(self.scan_all_btn)

        shared_panel = QVBoxLayout()  # Label → list → buttons
        shared_panel.setSpacing(4)
        shared_panel.addWidget(QLabel("תיקיות לסריקה:"))
        shared_panel.addWidget(self.roots_list)
        shared_panel.addLayout(shared_btn_row)

        main_layout.addLayout(shared_panel)  # Shared panel sits above the progress bar and tabs

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

        # Tab 1 — File Explorer
        self.table = _make_table(  # Build the file explorer table using the shared helper
            ["", "שם", "גודל", "סוג", "תאריך שינוי", "תיקייה"],  # Col 0 = checkbox; then Name, Size, Type, Modified Date, Folder
            stretch_col=5,  # Stretch the Folder column to fill remaining width
        )
        self.table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)  # No blue row highlight — checkboxes are the selection mechanism
        self.table.cellDoubleClicked.connect(self._on_scan_row_double_clicked)  # Open the file when the user double-clicks any cell

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

        self.explorer_delete_btn = QPushButton("🗑 מחק נבחרים")  # Sends all checked files in the explorer to the Recycle Bin
        self.explorer_delete_btn.setProperty("danger", "true")
        self.explorer_delete_btn.setEnabled(False)  # Disabled until the table is populated with results
        self.explorer_delete_btn.clicked.connect(
            lambda: self._delete_checked_rows(self.table, name_col=1, folder_col=5)
        )

        explorer_footer = QHBoxLayout()  # Footer row: delete button pinned to the left (RTL)
        explorer_footer.addStretch()
        explorer_footer.addWidget(self.explorer_delete_btn)

        scan_tab_widget = QWidget()  # Container widget for the file explorer tab
        scan_tab_layout = QVBoxLayout(scan_tab_widget)  # Vertical layout stacks search bar, filter bar, table, footer
        scan_tab_layout.setContentsMargins(0, 4, 0, 0)  # Small top margin so the search bar doesn't touch the tab edge
        scan_tab_layout.setSpacing(4)  # Small gap between each row in the tab
        scan_tab_layout.addLayout(search_row)  # Search bar sits at the very top of the tab
        scan_tab_layout.addLayout(filter_bar)  # Filter bar sits directly below the search bar
        scan_tab_layout.addWidget(self.table, stretch=1)  # Table expands to fill all remaining vertical space
        scan_tab_layout.addLayout(explorer_footer)  # Delete button sits below the table

        self.tabs.addTab(scan_tab_widget, "סייר קבצים")  # Add the container as the first tab labelled "File Explorer"

        # Tab 2 — Storage Cleanup (root picker moved to the shared panel above the tabs)
        cleanup_tab_widget = QWidget()  # Container widget for the entire cleanup tab
        cleanup_tab_layout = QVBoxLayout(cleanup_tab_widget)  # Vertical layout for the cleanup tab sections
        cleanup_tab_layout.setContentsMargins(0, 4, 0, 0)  # Small top margin so content doesn't touch the tab edge
        cleanup_tab_layout.setSpacing(8)  # Gap between sections

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
        self.large_size_input = QLineEdit("100")  # Threshold in MB; default 100 MB
        self.large_size_input.setValidator(QIntValidator(1, 100_000))
        self.large_size_input.setFixedWidth(56)
        self.large_size_up = QPushButton("▲")
        self.large_size_up.setObjectName("stepperBtn")
        self.large_size_up.setFixedSize(20, 14)
        self.large_size_up.clicked.connect(
            lambda: self.large_size_input.setText(str(min(100_000, int(self.large_size_input.text() or "100") + 10)))
        )
        self.large_size_down = QPushButton("▼")
        self.large_size_down.setObjectName("stepperBtn")
        self.large_size_down.setFixedSize(20, 14)
        self.large_size_down.clicked.connect(
            lambda: self.large_size_input.setText(str(max(1, int(self.large_size_input.text() or "100") - 10)))
        )

        self.large_files_table = _make_table(  # Checkbox table that lists files exceeding the size threshold
            ["", "שם", "גודל", "תיקייה"],  # Col 0 = checkbox; then Name, Size, Folder
            stretch_col=3,  # Stretch the Folder column to fill remaining width
        )
        self.large_files_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)  # No blue row highlight — checkboxes are the selection mechanism
        self.large_files_table.setMinimumHeight(150)  # Ensure the table is usable even when no results have been loaded yet

        self.large_files_label = QLabel("0 קבצים גדולים | 0 MB")  # Summary line updated by _on_files_ready
        self.large_files_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align for RTL consistency

        self.large_delete_btn = QPushButton("🗑 מחק נבחרים")  # Sends checked large files to the Recycle Bin
        self.large_delete_btn.setProperty("danger", "true")
        self.large_delete_btn.setEnabled(False)  # Disabled until Task 8 populates the table with results
        self.large_delete_btn.clicked.connect(
            lambda: self._delete_checked_rows(self.large_files_table, name_col=1, folder_col=3)
        )

        large_header = QHBoxLayout()  # Header row: section title on the right, threshold control on the left (RTL)
        _lbl = QLabel("קבצים גדולים"); _lbl.setObjectName("sectionTitle"); large_header.addWidget(_lbl)  # Section title
        large_header.addStretch()  # Push the threshold control to the opposite end
        large_header.addWidget(QLabel("גודל מינימלי:"))
        large_header.addWidget(self.large_size_input)

        large_size_stepper = QVBoxLayout()  # Stack ▲ on top of ▼
        large_size_stepper.setSpacing(1)
        large_size_stepper.setContentsMargins(0, 0, 0, 0)
        large_size_stepper.addWidget(self.large_size_up)
        large_size_stepper.addWidget(self.large_size_down)
        large_header.addLayout(large_size_stepper)
        large_header.addWidget(QLabel("MB"))

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
        self.old_months_input = QLineEdit("6")  # Threshold in months; default 6 months
        self.old_months_input.setValidator(QIntValidator(1, 120))
        self.old_months_input.setFixedWidth(40)
        self.old_months_up = QPushButton("▲")
        self.old_months_up.setObjectName("stepperBtn")
        self.old_months_up.setFixedSize(20, 14)
        self.old_months_up.clicked.connect(
            lambda: self.old_months_input.setText(str(min(120, int(self.old_months_input.text() or "6") + 1)))
        )
        self.old_months_down = QPushButton("▼")
        self.old_months_down.setObjectName("stepperBtn")
        self.old_months_down.setFixedSize(20, 14)
        self.old_months_down.clicked.connect(
            lambda: self.old_months_input.setText(str(max(1, int(self.old_months_input.text() or "6") - 1)))
        )

        self.old_files_table = _make_table(  # Checkbox table that lists files older than the threshold
            ["", "שם", "גודל", "תאריך שינוי", "תיקייה"],  # Col 0 = checkbox; then Name, Size, Modified Date, Folder
            stretch_col=4,  # Stretch the Folder column to fill remaining width
        )
        self.old_files_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)  # No blue row highlight — checkboxes are the selection mechanism
        self.old_files_table.setMinimumHeight(150)  # Ensure the table is usable even when no results have been loaded yet

        self.old_files_label = QLabel("0 קבצים ישנים | 0 MB")  # Summary line updated by _on_files_ready
        self.old_files_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align for RTL consistency

        self.old_delete_btn = QPushButton("🗑 מחק נבחרים")  # Sends checked old files to the Recycle Bin
        self.old_delete_btn.setProperty("danger", "true")
        self.old_delete_btn.setEnabled(False)  # Disabled until Task 8 populates the table with results
        self.old_delete_btn.clicked.connect(
            lambda: self._delete_checked_rows(self.old_files_table, name_col=1, folder_col=4)
        )

        old_header = QHBoxLayout()  # Header row: section title on the right, threshold control on the left (RTL)
        _lbl = QLabel("קבצים ישנים"); _lbl.setObjectName("sectionTitle"); old_header.addWidget(_lbl)  # Section title
        old_header.addStretch()  # Push the threshold control to the opposite end
        old_header.addWidget(QLabel("לא שונה מזה:"))
        old_header.addWidget(self.old_months_input)

        old_months_stepper = QVBoxLayout()  # Stack ▲ on top of ▼
        old_months_stepper.setSpacing(1)
        old_months_stepper.setContentsMargins(0, 0, 0, 0)
        old_months_stepper.addWidget(self.old_months_up)
        old_months_stepper.addWidget(self.old_months_down)
        old_header.addLayout(old_months_stepper)
        old_header.addWidget(QLabel("חודשים"))

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
        _lbl = QLabel("תיקיות כבדות"); _lbl.setObjectName("sectionTitle"); heavy_header.addWidget(_lbl)  # Section title: "Heavy Folders"
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

        self.cleanup_dup_delete_btn = QPushButton("🗑 מחק נבחרים")  # Sends checked duplicate copies to the Recycle Bin
        self.cleanup_dup_delete_btn.setProperty("danger", "true")
        self.cleanup_dup_delete_btn.setEnabled(False)  # Disabled until _populate_cleanup_duplicates finds at least one group
        self.cleanup_dup_delete_btn.clicked.connect(
            lambda: self._delete_checked_rows(self.cleanup_dup_table, name_col=1, folder_col=3)
        )

        dup_cleanup_header = QHBoxLayout()  # Header row: section title on the right (RTL); no threshold control needed
        _lbl = QLabel("קבצים כפולים"); _lbl.setObjectName("sectionTitle"); dup_cleanup_header.addWidget(_lbl)  # Section title: "Duplicate Files"
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
        self.status_label.setObjectName("statusLabel")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignRight)  # Right-align for RTL consistency

        main_layout.addWidget(self.status_label)  # Add the status label at the bottom of the layout

        self._apply_stylesheet(False)  # Apply the default light theme on startup

    # --- Theme ---

    def _apply_stylesheet(self, dark: bool):
        if dark:
            bg          = "#1A1A2E"
            surface     = "#16213E"
            primary     = "#2196F3"
            primary_dk  = "#1565C0"
            primary_lt  = "#0D47A1"
            text        = "#E3F2FD"
            text_muted  = "#90CAF9"
            border      = "#1565C0"
            danger      = "#EF5350"
            danger_dk   = "#C62828"
            input_bg    = "#16213E"
            header_bg   = "#0D47A1"
            scroll_bg   = "#16213E"
            scroll_handle = "#1565C0"
        else:
            bg          = "#FFFFFF"
            surface     = "#F5F9FF"
            primary     = "#2196F3"
            primary_dk  = "#1565C0"
            primary_lt  = "#E3F2FD"
            text        = "#212121"
            text_muted  = "#546E7A"
            border      = "#BBDEFB"
            danger      = "#E53935"
            danger_dk   = "#B71C1C"
            input_bg    = "#FFFFFF"
            header_bg   = "#E3F2FD"
            scroll_bg   = "#F5F9FF"
            scroll_handle = "#BBDEFB"

        QApplication.instance().setStyleSheet(f"""
            /* ── Base ── */
            QMainWindow, QWidget {{
                background-color: {bg};
                color: {text};
                font-family: Segoe UI, Arial, sans-serif;
                font-size: 10pt;
            }}

            /* ── Buttons — primary (blue) ── */
            QPushButton {{
                background-color: {primary};
                color: #FFFFFF;
                border: none;
                border-radius: 6px;
                padding: 6px 14px;
                font-size: 10pt;
            }}
            QPushButton:hover {{
                background-color: {primary_dk};
            }}
            QPushButton:pressed {{
                background-color: {primary_dk};
                padding: 7px 13px 5px 15px;
            }}
            QPushButton:disabled {{
                background-color: #9E9E9E;
                color: #E0E0E0;
            }}

            /* ── Buttons — danger (red delete buttons) ── */
            QPushButton[danger="true"] {{
                background-color: {danger};
            }}
            QPushButton[danger="true"]:hover {{
                background-color: {danger_dk};
            }}
            QPushButton[danger="true"]:disabled {{
                background-color: #9E9E9E;
                color: #E0E0E0;
            }}

            /* ── Progress bar ── */
            QProgressBar {{
                background-color: {surface};
                border: 1px solid {border};
                border-radius: 6px;
                height: 14px;
                text-align: center;
                color: {text};
                font-size: 9pt;
            }}
            QProgressBar::chunk {{
                background-color: {primary};
                border-radius: 6px;
            }}

            /* ── Tabs ── */
            QTabWidget::pane {{
                border: 1px solid {border};
                border-radius: 4px;
                background-color: {bg};
            }}
            QTabBar::tab {{
                background-color: {bg};
                color: {text_muted};
                padding: 8px 18px;
                border: none;
                border-bottom: 3px solid transparent;
                font-size: 10pt;
            }}
            QTabBar::tab:selected {{
                color: {primary};
                border-bottom: 3px solid {primary};
                font-weight: bold;
            }}
            QTabBar::tab:hover:!selected {{
                background-color: {primary_lt};
                color: {text};
            }}

            /* ── Tables ── */
            QTableWidget {{
                background-color: {bg};
                alternate-background-color: {surface};
                gridline-color: {border};
                border: 1px solid {border};
                border-radius: 4px;
                selection-background-color: {primary_lt};
                selection-color: {text};
            }}
            QTableWidget::item {{
                padding: 4px 6px;
            }}
            QTableWidget::indicator {{
                width: 14px;
                height: 14px;
                border: 2px solid {text_muted};
                border-radius: 3px;
                background-color: transparent;
            }}
            QTableWidget::indicator:checked {{
                background-color: {primary};
                border-color: {primary};
            }}
            QTableWidget::indicator:hover {{
                border-color: {primary};
            }}
            QHeaderView::section {{
                background-color: {header_bg};
                color: {text};
                padding: 6px 8px;
                border: none;
                border-right: 1px solid {border};
                font-weight: bold;
                font-size: 9pt;
            }}
            QHeaderView::section:last {{
                border-right: none;
            }}

            /* ── List widget (roots list) ── */
            QListWidget {{
                background-color: {input_bg};
                border: 1px solid {border};
                border-radius: 4px;
                padding: 2px;
                color: {text};
            }}
            QListWidget::item:selected {{
                background-color: {primary};
                color: #FFFFFF;
                border-radius: 3px;
            }}
            QListWidget::item:hover:!selected {{
                background-color: {primary_lt};
            }}

            /* ── Inputs ── */
            QLineEdit, QComboBox {{
                background-color: {input_bg};
                color: {text};
                border: 1px solid {border};
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 10pt;
            }}
            QLineEdit:focus, QComboBox:focus {{
                border: 1px solid {primary};
            }}
            QComboBox::drop-down {{
                border: none;
                width: 20px;
            }}

            /* ── Stepper buttons (▲▼ next to threshold inputs) ── */
            QPushButton#stepperBtn {{
                background-color: {header_bg};
                color: {text};
                border: 1px solid {border};
                border-radius: 2px;
                padding: 0px;
                font-size: 7pt;
            }}
            QPushButton#stepperBtn:hover {{
                background-color: {primary_lt};
                border-color: {primary};
            }}
            QPushButton#stepperBtn:pressed {{
                background-color: {border};
            }}

            /* ── Theme toggle button ── */
            QPushButton#themeBtn {{
                background-color: transparent;
                color: {text};
                border: 1px solid {border};
                border-radius: 6px;
                padding: 0px;
                font-size: 16pt;
            }}
            QPushButton#themeBtn:hover {{
                background-color: {primary_lt};
            }}

            /* ── Neutral button (הסר) ── */
            QPushButton#neutralBtn {{
                background-color: {bg};
                color: {text};
                border: 1px solid {border};
                border-radius: 6px;
                padding: 6px 14px;
            }}
            QPushButton#neutralBtn:hover {{
                background-color: {surface};
                border-color: {primary};
            }}
            QPushButton#neutralBtn:disabled {{
                background-color: {surface};
                color: {text_muted};
                border-color: {border};
            }}

            /* ── Warning button (עצור) ── */
            QPushButton#warningBtn {{
                background-color: {bg};
                color: {text};
                border: 2px solid #E65100;
                border-radius: 6px;
                padding: 6px 14px;
            }}
            QPushButton#warningBtn:hover {{
                background-color: #FFF3E0;
                border-color: #BF360C;
            }}
            QPushButton#warningBtn:disabled {{
                background-color: {surface};
                color: {text_muted};
                border: 1px solid {border};
            }}

            /* ── Scroll bars ── */
            QScrollBar:vertical {{
                background-color: {scroll_bg};
                width: 8px;
                border-radius: 4px;
            }}
            QScrollBar::handle:vertical {{
                background-color: {scroll_handle};
                border-radius: 4px;
                min-height: 24px;
            }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
                height: 0px;
            }}
            QScrollBar:horizontal {{
                background-color: {scroll_bg};
                height: 8px;
                border-radius: 4px;
            }}
            QScrollBar::handle:horizontal {{
                background-color: {scroll_handle};
                border-radius: 4px;
                min-width: 24px;
            }}
            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
                width: 0px;
            }}

            /* ── Labels ── */
            QLabel {{
                color: {text};
            }}
            QLabel#sectionTitle {{
                color: {text};
                font-size: 11pt;
                font-weight: bold;
            }}

            /* ── Status bar label ── */
            QLabel#statusLabel {{
                color: {text_muted};
                font-size: 9pt;
                padding: 2px 4px;
            }}
        """)

    def _toggle_theme(self):
        self.dark_mode = not self.dark_mode
        self.theme_btn.setText("☀️" if self.dark_mode else "🌙")
        self._apply_stylesheet(self.dark_mode)

    # --- Shared root picker ---

    def _on_add_root(self):  # Called when the user clicks "הוסף תיקייה" in the shared panel
        path = QFileDialog.getExistingDirectory(
            self,
            "בחר תיקייה לסריקה",  # "Select folder to scan"
            "",
        )
        if not path:
            return

        existing = [self.roots_list.item(i).text()
                    for i in range(self.roots_list.count())]
        if path in existing:  # Skip duplicates — the same root cannot appear twice
            return

        self.roots_list.addItem(path)
        self._on_roots_changed()

    def _on_remove_root(self):  # Called when the user clicks "הסר" in the shared panel
        selected = self.roots_list.selectedItems()
        for item in selected:
            self.roots_list.takeItem(self.roots_list.row(item))
        self._on_roots_changed()

    def _on_roots_selection_changed(self):  # Called whenever the selection in the shared roots list changes
        has_selection = bool(self.roots_list.selectedItems())
        self.remove_root_btn.setEnabled(has_selection)

    def _on_roots_changed(self):  # Updates button states whenever the shared roots list contents change
        has_roots = self.roots_list.count() > 0
        self.scan_all_btn.setEnabled(has_roots)

    # --- Unified scan flow ---

    def _start_scan(self):  # Called when the user clicks "סרוק הכל" in the shared panel
        roots = [
            self.roots_list.item(i).text()
            for i in range(self.roots_list.count())
        ]

        self.scan_all_btn.setEnabled(False)    # Prevent a second scan while one is already running
        self.add_root_btn.setEnabled(False)    # Root list must not change mid-scan
        self.remove_root_btn.setEnabled(False)
        self.explorer_delete_btn.setEnabled(False)  # Disable while results are being replaced

        self.progress_bar.setMaximum(0)  # Indeterminate (pulsing) mode until total_files arrives
        self.progress_bar.setValue(0)
        self.progress_bar.show()
        self.status_label.setText("סורק...")  # "Scanning…"

        self.worker = ScanWorker(roots)  # CRITICAL: stored on self so it is not garbage-collected
        self.worker.total_files.connect(self.progress_bar.setMaximum)   # Switch bar to percentage mode
        self.worker.progress.connect(self.progress_bar.setValue)         # Advance bar per file (scan pass)
        self.worker.progress.connect(self._on_scan_progress)            # Update status label with live count
        self.worker.files_ready.connect(self._on_files_ready)           # Tab 1 + Tab 2 large/old/heavy populate
        self.worker.hash_start.connect(self._on_hash_start)             # Reset bar for the hash pass
        self.worker.hash_progress.connect(self.progress_bar.setValue)   # Advance bar per file (hash pass only)
        self.worker.duplicates_ready.connect(self._on_duplicates_ready) # Tab 2 dup section populates
        self.worker.stopped.connect(self._on_scan_stopped)              # User cancelled mid-scan
        self.worker.error.connect(self._on_scan_error)
        self.stop_btn.setEnabled(True)  # Allow the user to cancel now that the worker exists
        self.worker.start()  # All signals connected — safe to launch the background thread

    def _on_scan_progress(self, count: int):  # Called on the main thread after each file is discovered
        self.status_label.setText(f"סורק... {count} קבצים")

    def _on_files_ready(self, files: list[dict]):  # Called when the scan pass finishes — hashing has not yet begun
        self._populate_scan_table(files)          # Fill Tab 1; also calls _apply_filters → updates status label
        self.tabs.setCurrentIndex(0)              # Switch to the file explorer so results are immediately visible
        self._populate_large_files(files)         # Fill Tab 2 large-files section
        self._populate_old_files(files)           # Fill Tab 2 old-files section
        self._populate_heavy_folders(files)       # Fill Tab 2 heavy-folders section
        # Progress bar stays visible — the hash pass is about to begin

    def _on_hash_start(self, total: int):  # Called just before hashing begins
        self.progress_bar.setMaximum(total)  # Reset bar maximum to file count for the hash pass
        self.progress_bar.setValue(0)        # Sweep from 0 % again during hashing
        self.status_label.setText("מחשב כפילויות...")  # "Computing duplicates…" — stays fixed while hash_progress silently drives the bar

    def _on_duplicates_ready(self, duplicates: dict):  # Called when hashing finishes — all data is now available
        self.progress_bar.hide()
        self.stop_btn.setEnabled(False)
        self.scan_all_btn.setEnabled(True)
        self.add_root_btn.setEnabled(True)
        self._on_roots_changed()                     # Re-evaluate Remove button based on current list contents
        self._populate_cleanup_duplicates(duplicates)  # Fill Tab 2 duplicate-files section
        total = self.table.rowCount()                # Total files from the completed scan
        self.status_label.setText(f"סיום סריקה — {total} קבצים")  # "Scan complete — X files"

    def _on_scan_error(self, message: str):  # Called if the worker raises an unrecoverable exception
        self.progress_bar.hide()
        self.stop_btn.setEnabled(False)
        self.scan_all_btn.setEnabled(True)
        self.add_root_btn.setEnabled(True)
        self._on_roots_changed()
        self.status_label.setText(f"שגיאה: {message}")

    def _on_scan_stopped(self):  # Called when the user cancels mid-scan via the stop button
        self.progress_bar.hide()
        self.stop_btn.setEnabled(False)
        self.scan_all_btn.setEnabled(True)
        self.add_root_btn.setEnabled(True)
        self._on_roots_changed()                     # Re-evaluate Remove button based on current list contents
        # Tab 1 results already populated (if files_ready fired) — leave them intact
        self.status_label.setText("סריקה הופסקה")  # "Scan stopped"

    # --- File opening ---

    def _open_file(self, folder: str, name: str):  # Build the full path and open the file with its default application
        full_path = os.path.join(folder, name)  # Combine folder and name into an absolute path
        try:
            os.startfile(full_path)  # Ask Windows to open the file with whatever app is registered for its type
        except OSError as e:  # Catch errors such as file not found or no associated application
            self.status_label.setText(f"שגיאה בפתיחת קובץ: {e}")  # Show the error in the status bar so the user knows what went wrong

    def _on_scan_row_double_clicked(self, row: int, _col: int):  # Called when the user double-clicks a row in the file explorer table
        name   = self.table.item(row, 1).text()  # Column 1 holds the file name (col 0 is the checkbox)
        folder = self.table.item(row, 5).text()  # Column 5 holds the parent folder path
        self._open_file(folder, name)  # Delegate to the shared open helper

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

        for row in range(self.table.rowCount()):  # Check every row in the file explorer table
            name_item = self.table.item(row, 1)  # Col 1: name cell holds the raw file dict in UserRole
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

    def _populate_scan_table(self, files: list[dict]):  # Fill the file explorer table with a list of file dicts
        self.table.setRowCount(len(files))  # Resize the table to match the number of files

        for row, file in enumerate(files):  # Iterate over each file dict with its row index
            chk_item = QTableWidgetItem()  # Col 0: checkbox — no display text
            chk_item.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)  # Checkable but not text-editable
            chk_item.setCheckState(Qt.CheckState.Unchecked)  # Start every row unticked
            chk_item.setData(Qt.ItemDataRole.UserRole, file["size_bytes"])  # Store raw bytes so _delete_checked_rows can total them

            name_item = QTableWidgetItem(file["name"])  # Col 1: file name
            name_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align
            name_item.setData(Qt.ItemDataRole.UserRole, file)  # Store the full file dict so _apply_filters can read raw values

            size_item = QTableWidgetItem(format_size(file["size_bytes"]))  # Col 2: formatted size
            size_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)  # Center-align

            type_item = QTableWidgetItem(file["file_type"] or "—")  # Col 3: extension or dash if none
            type_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)  # Center-align

            date_item = QTableWidgetItem(file["modified_date"].strftime("%d/%m/%Y"))  # Col 4: date formatted as DD/MM/YYYY
            date_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)  # Center-align

            folder_item = QTableWidgetItem(file["folder"])  # Col 5: parent folder path
            folder_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)  # Right-align

            self.table.setItem(row, 0, chk_item)    # Checkbox
            self.table.setItem(row, 1, name_item)   # Name
            self.table.setItem(row, 2, size_item)   # Size
            self.table.setItem(row, 3, type_item)   # Type
            self.table.setItem(row, 4, date_item)   # Date
            self.table.setItem(row, 5, folder_item) # Folder

        self.explorer_delete_btn.setEnabled(len(files) > 0)  # Enable the delete button now that there are rows to act on
        self._apply_filters()  # Apply any active filters immediately and update the status bar with the visible count

    def _populate_large_files(self, files: list[dict]):  # Fill the Large Files table with files that meet or exceed the size threshold
        threshold_bytes = int(self.large_size_input.text() or "100") * 1_048_576  # Convert the MB input to bytes for comparison

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
        months = int(self.old_months_input.text() or "6")
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

    def _populate_cleanup_duplicates(self, duplicates: dict[str, list[dict]]):  # Fill the Duplicate Files table from the hash→files mapping produced by ScanWorker
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
