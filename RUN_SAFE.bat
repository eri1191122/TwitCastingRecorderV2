#!/usr/bin/env python
import sys
import subprocess

# Python 3.6以上が必要
if sys.version_info < (3, 6):
    print("Python 3.6以上が必要です。")
    print(f"現在のバージョン: {sys.version}")
    
    # python3 コマンドを試す
    try:
        subprocess.run(["python3", "tc_recorder_gui.py"])
    except FileNotFoundError:
        print("\nPython 3をインストールしてください:")
        print("https://www.python.org/downloads/")
        input("Press Enter to exit...")
else:
    # Python 3.6以上なら直接import
    from tc_recorder_gui import RecorderGUI
    RecorderGUI().run()
