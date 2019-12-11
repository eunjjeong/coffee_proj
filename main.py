from PyQt5.QtWidgets import QApplication
from database_setting.widget_coffee_setting import WidgetCoffeeSetting


def setting_main():
    app = QApplication([])
    w = WidgetCoffeeSetting()
    app.exec_()


if __name__ == "__main__":
    setting_main()