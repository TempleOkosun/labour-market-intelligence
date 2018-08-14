import time
import tkinter as tk  # Tkinter is used so that file import/export is simplified and chances of errors minimized.
from tkinter.filedialog import askopenfilename
import pandas as pd


def file_path_finder():
    """
    This function will open a dialog box to help get the JSON file path.

    Input:
        User selects JSON file through a dialog box.

    Returns:
        The exact file path is returned as a String to be used by main loader function.
    """
    input('Press Enter to browse to the file location:\n')  # Pause to ensure user is in control.
    root = tk.Tk()
    root.withdraw()  # Hides the root frame. Else, a frame will be hanging around and can lead to a crash.
    root.update()  # may be necessary to force the hiding of the main frame.
    file_name = askopenfilename()
    root.destroy()
    return file_name


data = file_path_finder()
print(data)
data = pd.read_json(data, lines=True)
data.info()
data