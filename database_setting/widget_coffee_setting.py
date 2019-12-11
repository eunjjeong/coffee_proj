from PyQt5 import uic
from PyQt5.QtWidgets import QWidget, QMessageBox

from db_connection.coffee_init_service import DBInitService


class WidgetCoffeeSetting(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("COFFEE")
        self.ui = uic.loadUi("database_setting/coffee.ui")

        self.ui.btn_reset.clicked.connect(self.reset)
        self.ui.btn_reset.setShortcut("Ctrl+R") # 단축키
        self.ui.btn_reset.setToolTip("단축키는 Ctrl+R")

        self.ui.btn_backup.clicked.connect(self.backup)
        self.ui.btn_backup.setShortcut("Ctrl+S")
        self.ui.btn_backup.setToolTip("단축키는 Ctrl+S")

        self.ui.btn_restore.clicked.connect(self.restore)
        self.ui.btn_restore.setShortcut("Ctrl+B")
        self.ui.btn_restore.setToolTip("단축키는 Ctrl+B")

        self.ui.show()
        self.db = DBInitService()


    def reset(self):
        self.db.service()
        QMessageBox.about(self, "RESET", "초기화되었습니다.")

    def backup(self):
        self.db.data_backup("sale")
        self.db.data_backup("product")
        QMessageBox.about(self, "BACKUP", "백업되었습니다.")

    def restore(self):
        self.db.data_restore("product")
        self.db.data_restore("sale")
        QMessageBox.about(self, "RESTORE", "복원되었습니다.")
