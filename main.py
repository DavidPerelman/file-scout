import sys   # Import sys to access command-line arguments and exit functionality
import os    # Import os for os.startfile() to open files with their default application
from PyQt6.QtWidgets import (  # Import all needed Qt widgets
    QApplication, QMainWindow, QWidget,  # Core window and container widgets
    QVBoxLayout, QHBoxLayout,  # Vertical and horizontal layout managers
    QPushButton, QLabel,  # Button and text label widgets
    QTableWidget, QTableWidgetItem,  # Table widget and its cell item class
    QHeaderView, QFileDialog,  # Header behavior control and folder picker dialog
    QTabWidget, QProgressBar, QMessageBox,  # Tab container, progress bar, and confirmation dialog widgets
    QComboBox, QLineEdit  # Dropdown selector and single-line text input for the filter bar
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal  # Import Qt namespace, thread class, and signal type
from PyQt6.QtGui import (  # Import GUI-level classes
    QIntValidator,  # Restricts a QLineEdit to integer input only
    QColor,         # Used to set the yellow highlight background on matching filename cells
    QBrush,         # Wraps QColor into a brush accepted by QTableWidgetItem.setBackground()
)
from datetime import datetime  # Import datetime for comparing file modification dates in the filter
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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

app = QApplication(sys.argv)  # Create the Qt application, passing command-line arguments
app.setLayoutDirection(Qt.LayoutDirection.RightToLeft)  # Set RTL direction globally for all widgets

window = MainWindow()  # Instantiate the main window
window.show()  # Make the window visible on screen

sys.exit(app.exec())  # Start the event loop and exit cleanly when the app closes
