import os
import sys
import time
import pandas as pd
from PyQt6.QtCore import (
    Qt, QAbstractTableModel, QSortFilterProxyModel,
    QModelIndex, QThread, pyqtSignal, QTimer, QUrl,
    QObject, QRunnable, QThreadPool, pyqtSlot
)
from PyQt6.QtGui import QPixmap, QDesktopServices, QIcon
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QTableView,
    QLabel, QLineEdit, QHeaderView, QAbstractItemView, QPushButton,
    QSplitter, QScrollArea, QSplashScreen, QFileDialog, QMessageBox,
    QProgressBar, QSizePolicy
)

from main import start_scan, load_cache, find_pdfs_to_process
from data_io import load_data, save_data, load_last_folder, save_last_folder

import sys_cache
sys_cache.check_usage()

ICON_PATH = os.path.join(os.path.dirname(__file__), "icons", "logo.ico")

# --------------------------- THREAD FOR SCANNING ---------------------------
class ScanThread(QThread):
    progress_signal = pyqtSignal(int, int)  # current, total
    completed_signal = pyqtSignal(pd.DataFrame)

    def __init__(self, folder_path):
        super().__init__()
        self.folder_path = folder_path

    def run(self):
        try:
            print(f"📂 Starting scan in: {self.folder_path}")

            # Define a progress callback
            def progress_callback(current, total):
                self.progress_signal.emit(current, total)

            # Prepare for scan
            cache = load_cache()
            jobs = find_pdfs_to_process(self.folder_path, cache)
            total_jobs = len(jobs)

            print(f"📁 Found {total_jobs} new/updated PDFs.")

            # Start scan with callback
            df = start_scan(self.folder_path, on_progress=progress_callback)

            print("✅ Scan completed.")
            self.completed_signal.emit(df if df is not None else pd.DataFrame())
        except Exception as e:
            print(f"❌ Exception in thread: {e}")

# --------------------------- PANDAS TABLE MODEL ---------------------------
class PandasModel(QAbstractTableModel):
    def __init__(self, df):
        super().__init__()
        self._df = df


    def rowCount(self, parent=QModelIndex()):
        return self._df.shape[0]

    def columnCount(self, parent=QModelIndex()):
        return self._df.shape[1]

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.ItemDataRole.DisplayRole:
            value = self._df.iloc[index.row(), index.column()]
            return str(value)
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                return self._df.columns[section]
            else:
                return self._df.iloc[section]["Index"]
        return None

# --------------------------- WORKER FOR THREADPOOL ---------------------------
class WorkerSignals(QObject):
    progress = pyqtSignal(int, int) # Emits current progress and total

class Worker(QRunnable):
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self):
        try:
            # Call the provided function with arguments
            result = self.fn(*self.args, **self.kwargs, progress_callback=self.signals.progress)
        except Exception as e:
            print(f"⚠️ Error in Worker: {e}")

    # --------------------------- MAIN BOOK BROWSER CLASS ---------------------------

class BookBrowser(QWidget):
    def __init__(self, folder_path):
        super().__init__()
        self.folder_path = folder_path
        self.scan_thread = None
        self.df = pd.DataFrame()
        self.current_file_path = ""

        self.setWindowTitle("MetaSortX")
        self.setGeometry(100, 100, 1600, 800)

        self.init_ui()

        # Start scanning right away using ScanThread
        self.start_scan()

    def init_ui(self):
        self.main_layout = QVBoxLayout(self)
        content_layout = QHBoxLayout()
        ICON_PATH = ICON_PATH = os.path.join(os.path.dirname(__file__), "icons", "logo.ico")
        splitter = QSplitter(Qt.Orientation.Horizontal)
        self.setWindowIcon(QIcon(ICON_PATH))
        # self.setWindowOpacity(0.95)
        self.setStyleSheet("""
            QWidget { background-color: #1e1e1e; color: #eeeeee; font-family: 'Segoe UI'; font-size: 10pt; }
            QLineEdit { background-color: #2e2e2e; color: white; padding: 5px; border: 1px solid rgba(255,255,255,0.2); border-radius: 4px; }
            QPushButton { background-color: #333; color: white; padding: 6px; border-radius: 4px; }
            QPushButton:hover { background-color: #555; }
            QLabel { color: #dddddd; }
        """)

        left_widget = QWidget()
        right_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        right_layout = QVBoxLayout(right_widget)

        self.change_folder_button = QPushButton("📁 Change Folder")
        self.change_folder_button.clicked.connect(self.change_folder)
        right_layout.addWidget(self.change_folder_button)

        self.search_bar = QLineEdit()
        self.search_bar.setPlaceholderText("🔍 Search...")
        self.search_bar.setClearButtonEnabled(True)
        self.search_bar.textChanged.connect(self.filter_table)
        left_layout.addWidget(self.search_bar)

        self.model = PandasModel(self.df)
        self.proxy_model = QSortFilterProxyModel()
        self.proxy_model.setSourceModel(self.model)
        self.proxy_model.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.proxy_model.setFilterKeyColumn(-1)

        self.table = QTableView()

        # Set the model
        self.table.setModel(self.proxy_model)

        # Left-align horizontal headers
        self.table.horizontalHeader().setDefaultAlignment(
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
        )

        # Selection behavior: full row, single selection
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)

        # Disable cell editing (optional, improves selection behavior)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        # Connect selection change to preview update
        self.table.selectionModel().selectionChanged.connect(self.update_preview)

        # Resize behavior
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setStretchLastSection(True)

        # Expanding layout policy
        self.table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        # Add to layout
        left_layout.addWidget(self.table, stretch=1)


        # Optional: Pre-select the first row
        self.table.selectRow(0)

        self.preview_label = QLabel("📖 Thumbnail will appear here")
        self.preview_label.setFixedSize(300, 400)
        self.preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.preview_label.setStyleSheet(
            "border: 1px solid rgba(255,255,255,0.25); background-color: black; color: white")

        self.info_label = QLabel()
        self.info_label.setWordWrap(True)
        self.info_label.setStyleSheet(
            "color: white; background-color: black; border-top: 1px solid rgba(255,255,255,0.2); padding-top: 8px;")

        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        scroll_layout.addWidget(self.preview_label, alignment=Qt.AlignmentFlag.AlignHCenter)
        scroll_layout.addWidget(self.info_label)
        scroll_layout.addStretch()

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(scroll_content)
        scroll_area.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        right_layout.addWidget(scroll_area, stretch=1)

        # Buttons
        self.refresh_button = QPushButton("🔄 Refresh")
        self.refresh_button.clicked.connect(self.start_scan)

        self.open_button = QPushButton("📂 Open Book")
        self.open_button.clicked.connect(self.open_book)

        # Spacer to push buttons to bottom
        button_spacer = QWidget()
        button_spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        right_layout.addWidget(button_spacer)

        # Layout for bottom-aligned buttons
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.refresh_button)
        button_layout.addWidget(self.open_button)

        right_layout.addLayout(button_layout)

        splitter.addWidget(left_widget)
        splitter.addWidget(right_widget)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)

        content_layout.addWidget(splitter)


        self.main_layout.addLayout(content_layout)
        self.pdf_count_label = QLabel("Loading...")
        self.pdf_count_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.pdf_count_label.setFixedHeight(24)
        self.pdf_count_label.setStyleSheet(
            "color: #cccccc; background-color: #2c2c2c; border-top: 1px solid #444; font-size: 10pt;")

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setFixedHeight(10)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: none;
                background-color: #f0f0f0;
            }
            QProgressBar::chunk {
                background-color: #5a9bd5;
            }
        """)

        self.total_label = QLabel("Total PDFs: 0")
        self.total_label.setSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Fixed)

        bottom_layout = QHBoxLayout()
        # self.main_layout.addStretch(1)
        self.main_layout.addWidget(self.pdf_count_label)
        bottom_layout.addWidget(self.total_label)
        bottom_layout.addWidget(self.progress_bar)

        self.main_layout.addLayout(bottom_layout)

    def start_scan(self):
        if getattr(self, 'scan_thread') and self.scan_thread.isRunning():
            print("Scan already in progress, please wait...")
            return  # Don't start a new scan if one is active

        self.info_label.setText("⏳ Scanning PDF folder... Please wait.")
        self.progress_bar.setVisible(True)
        print("📡 Starting scan thread...")

        self.scan_thread = ScanThread(self.folder_path)
        self.scan_thread.progress_signal.connect(self.on_scan_progress)
        self.scan_thread.completed_signal.connect(self.on_progress)
        self.scan_thread.completed_signal.connect(lambda _: self.info_label.setText("✅ Scan completed."))

        def clean_up_thread():
            self.scan_thread.deleteLater()
            self.scan_thread = None  # Clear Python reference here!
        self.scan_thread.finished.connect(clean_up_thread)

        self.scan_thread.start()

    def on_scan_progress(self, current, total):
        if total > 0:
            progress_percent = int((current / total) * 100)
            self.progress_bar.setValue(progress_percent)
            self.total_label.setText(f"Scanning PDFs: {current} / {total}")
        if current == total:
            self.progress_bar.setVisible(False)
            self.total_label.setText(f"Total PDFs: {total}")

    def on_progress(self, new_df):
        new_df = new_df.fillna("")

        # Apply title case to string columns
        for col in new_df.select_dtypes(include='object').columns:
            new_df[col] = new_df[col].astype(str).str.title()

        # Add UniqueID for internal use (optional)
        new_df["UniqueID"] = new_df["Section"].apply(
            lambda p: os.path.join(os.path.basename(os.path.dirname(p)), os.path.basename(p))
        )

        self.df = new_df
        self.model = PandasModel(self.df)
        self.proxy_model.setSourceModel(self.model)
        self.table.setModel(self.proxy_model)

        # Reconnect selectionChanged signal to keep info panel updated
        try:
            self.table.selectionModel().selectionChanged.disconnect()
        except:
            pass

        # Auto-select first row after data is loaded
        if self.proxy_model.rowCount() > 0:
            index = self.proxy_model.index(0, 0)
            self.table.selectRow(index.row())

        self.table.selectionModel().selectionChanged.connect(self.update_preview)


        header = self.table.horizontalHeader()
        col_widths = {
            "Index":45,
            "ISBN":100,
            "File Name": 350,
            "Page Count": 80,
            "Year": 50,
            "Table of Contents":300
        }
        for col_name, width in col_widths.items():
            if col_name in self.df.columns:
                index = self.df.columns.get_loc(col_name)
                header.resizeSection(index, width)

        columns_to_hide = ["Preview Image", "Description", "Has Bookmarks", "UniqueID", "Absolute Path", "Read Status", "Path"]
        for col_name in columns_to_hide:
            if col_name in self.df.columns:
                col_index = self.df.columns.get_loc(col_name)
                self.table.setColumnHidden(col_index, True)

        self.update_pdf_count()
        self.info_label.setText("✅ Scan complete. Displaying results.")

    def update_pdf_count(self):
        count = len(self.df)
        self.pdf_count_label.setText(f"📄 Total Books: {count}")

    def update_preview(self, selected, _):
        indexes = selected.indexes()
        if not indexes:
            return

        proxy_index = indexes[0]
        source_index = self.proxy_model.mapToSource(proxy_index)
        row = source_index.row()

        if row < 0 or row >= len(self.df):
            return  # safety check

        preview_data = self.df.iloc[row]
        self.current_file_path = preview_data.get("Absolute Path", "")
        self.update_preview_image(self.current_file_path)

        # Build the details string
        details = f"""
            <b>📄 File Name:</b> {preview_data['File Name'].title()}<br>
            <b>📝 Author:</b> {preview_data['Author'].title()}<br>
            <b>📅 Year:</b> {preview_data['Year']}<br>
            <b>📃 Pages:</b> {preview_data['Page Count']}<br>
            <b>📚 Keywords:</b> {preview_data['Keywords'].title()}<br><br>
            <b>📑 Contents:</b><br>{preview_data['Table of Contents'].title().replace(';', '<br>')}<br><br>
        """
        self.info_label.setText(details)

    def change_folder(self):
        new_folder = QFileDialog.getExistingDirectory(self, "Select New PDF Folder", self.folder_path)
        if not new_folder:
            return

        if os.path.normpath(new_folder) != os.path.normpath(self.folder_path):
            for f in ["Books_Data.csv", "last_scanned.json"]:
                if os.path.exists(f):
                    os.remove(f)

            self.folder_path = new_folder
            save_last_folder(new_folder)

            # Clear current data and view
            self.df = pd.DataFrame()
            self.model = PandasModel(self.df)
            self.proxy_model.setSourceModel(self.model)
            self.table.setModel(self.proxy_model)
            self.info_label.setText("🔄 Folder changed. Scanning new folder...")
            self.preview_label.clear()
            self.preview_label.setText("📖 Thumbnail will appear here")
            self.current_file_path = ""

            self.start_scan()

    def filter_table(self, text):
        self.proxy_model.setFilterFixedString(text)

    def update_preview_image(self, img_path):
        try:
            if not os.path.exists(img_path):
                self.preview_label.clear()
                self.preview_label.setText("❌ Preview not found.")
                return

            pixmap = QPixmap(img_path)
            if pixmap.isNull():
                self.preview_label.clear()
                self.preview_label.setText("❌ Invalid image.")
                return

            # Get label size to scale dynamically
            label_size = self.preview_label.size()

            scaled_pixmap = pixmap.scaled(
                label_size,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation
            )

            self.preview_label.setPixmap(scaled_pixmap)

        except Exception as e:
            self.preview_label.clear()
            self.preview_label.setText("❌ Error loading preview.")
            print(f"Image load error: {e}")

    def open_book(self):
        if self.current_file_path:
            QDesktopServices.openUrl(QUrl.fromLocalFile(self.current_file_path))

# --------------------------- APP LAUNCHER ---------------------------
def launch_main_window():
    print("Launching the main window...")
    last_folder = load_last_folder()

    if last_folder and os.path.isdir(last_folder):
        folder = last_folder
        print(f"📂 Using previously selected folder: {folder}")
    else:
        folder = QFileDialog.getExistingDirectory(None, "Select PDF Folder", os.getcwd())
        if not folder:
            QMessageBox.critical(None, "No Folder Selected", "You must select a folder to continue.")
            return
        save_last_folder(folder)

    output_csv = os.path.join(folder, "Books_Data.csv")
    window = BookBrowser(folder)

    total_start = time.time()
    df, load_time, method = load_data(folder)

    if not df.empty:
        window.on_progress(df)
        total_elapsed = time.time() - total_start
        print(f"✅ Loaded data from {method} in {load_time:.3f}s")
        print(f"🧠 UI ready in {total_elapsed:.3f}s")

        QTimer.singleShot(1000, lambda: QMessageBox.information(
            window,
            "Load Performance",
            f"✅ Loaded data from {method} in {load_time:.3f} sec\n"
            f"🧠 UI fully ready in {total_elapsed:.3f} sec"
        ))
    else:
        window.start_scan()

    print("Window created. Showing...")
    window.show()
    return window

# --------------------------- MAIN EXECUTION ---------------------------
if __name__ == "__main__":
    try:
        print("Starting the application...")
        app = QApplication(sys.argv)

        def resource_path(relative_path):
            try:
                base_path = sys._MEIPASS
            except AttributeError:
                base_path = os.path.dirname(os.path.abspath(__file__))
            return os.path.join(base_path, relative_path)

        logo_path = resource_path("icons/logo.png")
        splash_pix = QPixmap(logo_path)

        if splash_pix.isNull():
            print("❌ Error: logo.png could not be loaded.")
            sys.exit(1)

        splash = QSplashScreen(splash_pix.scaled(450, 550, Qt.AspectRatioMode.KeepAspectRatio,
                                                 Qt.TransformationMode.SmoothTransformation))
        splash.showMessage("Loading MetaSort...", Qt.AlignmentFlag.AlignBottom | Qt.AlignmentFlag.AlignCenter,
                           Qt.GlobalColor.white)
        splash.show()

        def start_app():
            print("Splash closed. Opening main window...")
            splash.close()
            global main_window
            main_window = launch_main_window()
            if main_window is None:
                sys.exit(0)

        QTimer.singleShot(1500, start_app)
        sys.exit(app.exec())

    except Exception as e:
        print(f"❌ Error in application execution: {e}")
        sys.exit(1)
