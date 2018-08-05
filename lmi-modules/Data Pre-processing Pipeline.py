

import tkinter as tk # Tkinter is used so that file import/export is simplified and chances of errors minimized.
from tkinter.filedialog import askopenfilename
import pandas as pd
from pandas import DataFrame
import time


def data_loader():
    
    def greet():
        welcome_msg = ('Welcome to the Data Pre-processor of the LMI System. ' +
                       'We will begin by loading the data sets into memory.')        
        print(welcome_msg)
    
    greet() 
    time.sleep(0.20)

    # time.sleep(0.20)
    # Jupyter Notebook Print-vs-Input Workaround.
    # This will prevent the input statement below from executing before the print.
    # https://stackoverflow.com/questions/50439035/jupyter-notebook-input-line-executed-before-print-statement
    
    main_menu_msg = ('\nPlease select an option [1, 2 or 0 to exit]: ' +
                     '\n [1] To Import JSON data format.' +
                     '\n [2] To Import from MongoDB.' +
                     '\n [0] To Exit.' +
                     '\n ')        
    try:
        main_menu_option = int(input(main_menu_msg)) 
        if main_menu_option == 1:

            # Define a function-L1
            def json_file_handler():
                msg = '\nLoad JSON Documents.'
                print(msg)
                time.sleep(0.20)

                # A sub function-L2
                def json_file_path():
                    input('Press Enter to browse to the file location:\n')  # Pause to ensure user is in control.
                    root = tk.Tk()                             
                    root.withdraw() # Hides the root frame. Else, a frame will be hanging around.
                    root.update()  # may be necessary to force the hiding of the main frame.
                    file_name = askopenfilename()             
                    root.destroy()  
                    return file_name

                def json_structure():
                    json_structure_msg = ('\nPlease select an option [1 or 2]' +
                        ' to indicate how JSON Objects were organized in the file:\n' +
                        '  [1] An array of JSON objects seperated by a comma \',\' as shown below:\n' +
                        '    [{"Job_Id":"15","Location":"Aberdeen"}, {"Job_Id":"67","Location":"Dundee"}] \n\n' +
                        '  [2] One JSON object per line usually when JSON was exported from MongoDB as shown below:\n' +
                        '    NB-Anything within{} is treated as a single line even if the text spans multiple lines.\n' +
                        '    {"Job_Id":"11","Location":"Cork"}\n' +
                        '    {"Job_Id":"30","Location":"England"}\n')

                    json_structure_option = int(input(json_structure_msg))
                    return json_structure_option

                def load_data():
                    if json_structure_option == 1:
                        data = pd.read_json(path_or_buf=json_file_name, lines=False)

                    elif json_structure_option == 2:
                        data = pd.read_json(path_or_buf=json_file_name, lines=True)

                    return data
                
                json_file_name = json_file_path()
                print('The selected file is: ' + json_file_name)
                json_structure_option = json_structure()
                print('The JSON file structure type is: ' + str(json_structure_option))
                df = load_data()
                print('Details of the data:')
                print(df.info())

                #  Add another data i.e Merge rowise.
                print('\nData Merging Process')
                merge_data_menu = True # Show the menu for the first time alert.
                while merge_data_menu:
                    merge_data_msg = ('Would you like to add another data i.e. merge Rowise ?\n' +
                                      ' Please select an option [1 0r 2] \n' +
                                      ' [1] Yes- To select the data \n' +
                                      ' [2] No- To proceed with data preparation.\n')
                    merge_data_option = int(input(merge_data_msg))
                    if merge_data_option == 1:
                        new_df_to_merge_file_name = json_file_path()
                        json_structure_option = json_structure()
                        print('The JSON file structure is: ' + str(json_structure_option))
                        df_to_merge = load_data()
                        print(new_df_to_merge_file_name)
                        frames = [df_to_merge, df]
                        new_df = pd.concat(frames, ignore_index=True)
                        print('Details of the new data:')
                        print(new_df.info())

            #  Use the function
            json_file_handler()

    except ValueError:
            print('Input Error')

data_loader()

