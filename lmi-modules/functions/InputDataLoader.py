"""
The InputDataLoader module is a robust module with a nested-function to facilitate data loading.
Supports import of JSON file which could be arranged as (an array of JSON objects or JSON objects per line) in the file.
Supports import of data from MongoDB.

Returns:
    A pandas.DataFrame
"""


import time
import tkinter as tk  # Tkinter is used so that file import/export is simplified and chances of errors minimized.
from tkinter.filedialog import askopenfilename
import pandas as pd
from pymongo import MongoClient


# The main function to be called for data loading.
def data_loader_function():
    """
    This function will assist users to load data sets.

    Input:
        Users can selects JSON files through a dialog box or provide MongoDB connection details.

    Returns:
        A pandas.DataFrame
    """

    # sub-functions for JSON loading
    def json_file_path():
        """
        This sub-function will open a dialog box to get the JSON file path.

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

    def json_structure():
        """
        This sub-function will help determine JSON file structure i.e. how the JSON documents are
        separated within the file so that the file can be read in properly.

        Input:
            User provides an integer input.

        Returns:
            The JSON file structure type an Int [1 or 2 ] to be used by main loader function.
        """
        json_structure_msg = ('Please select an option [1 or 2]'
            ' to indicate how JSON Objects were organized in the file:\n'
            '  [1] An array of JSON objects separated by a comma \',\' as shown below:\n'
            '    [{"Job_Id":"15","Location":"Aberdeen"}, {"Job_Id":"67","Location":"Dundee"}] \n\n'
            '  [2] One JSON object per line usually when JSON was exported from MongoDB as shown below:\n'
            '    NB-Anything within{} is seen as a single line even if the text spans multiple lines.\n'
            '    {"Job_Id":"11","Location":"Cork"}\n'
            '    {"Job_Id":"30","Location":"England"}\n')
        json_file_structure_menu = True
        while json_file_structure_menu:
            try:
                json_structure_option = int(input(json_structure_msg))
            except ValueError:
                print('Whoops! That is not a number.')
                continue
            else:
                print('Ok.')
                json_file_structure_menu = False
            return json_structure_option

    def load_json_data(json_structure_option, json_file_name):
        """
        This sub-function will load the JSON file into memory using the appropriate loader based on
        the JSON file structure option and file path given.

        Input:
            json_file_structure_option: an integer [1 or 2].
            json_file_name: A String.

        Returns:
            A pandas.DataFrame loaded in memory for manipulations.
        """
        if json_structure_option == 1:
            data_frame = pd.read_json(path_or_buf=json_file_name, lines=False)
        else:
            data_frame = pd.read_json(path_or_buf=json_file_name, lines=True)
        return data_frame

    def add_another_data_handler():
        """
        This sub-function will ask if additional data is available to be merge.

        Input:
            An integer [1 or 2] representing to add another data or proceed respectively.

        Returns:
            An integer representing user choice is returned.
        """
        merge_data_msg = ('Would you like to add another data i.e. merge Row wise ?\n'
                          ' Please select an option [1 0r 2] \n'
                          ' [1] Yes- To select the data \n'
                          ' [2] No- To proceed with data preparation.\n')
        # ValueError exception handling.
        merge_data_msg_display = True
        while merge_data_msg_display:
            try:
                merge_data_option = int(input(merge_data_msg))
            except ValueError:
                print("Whoops! That's not a number.")
                continue
            else:
                print("Ok.")
                merge_data_msg_display = False
        return merge_data_option

    def json_file_handler(new_df=None):
        """
        This sub-function implements the actual user import process for json files. It uses other json sub-functions.
        It handles merging of additional data into a single DataFrame.

        Input:
            1: Users will be prompted to choose scraped data location.
            2: Users will be prompted to select json file structure.
            3: Users will be prompted for more files.

        Returns:
            A pandas.DataFrame loaded in memory for manipulations.
        """
        greet()
        msg = '\nLoad JSON Documents.'
        print(msg)
        time.sleep(0.20)
        json_file_name = json_file_path()
        json_structure_option = json_structure()
        print('The selected file is: ' + json_file_name)
        print('The JSON file structure type is: ' + str(json_structure_option))
        current_data_frame = load_json_data(json_structure_option, json_file_name)
        no_of_files = 1
        print('Total no. of data loaded: ' + str(no_of_files))
        print('Details of the current data:')
        print(current_data_frame.info())
        print(current_data_frame.head())

        #  Add another data i.e merge row wise.
        print('\nData Merging Process')
        merge_data_menu = True  # Show the menu for the first time alert.
        while merge_data_menu:
            merge_data_option = add_another_data_handler()
            # A new DataFrame i.e. new_df will be formed if there is a choice for add i.e.merge.
            if merge_data_option == 1:
                new_df_to_merge_file_name = json_file_path()
                new_json_structure_option = json_structure()
                print('The selected file is: ' + new_df_to_merge_file_name)
                print('The JSON file structure is: ' + str(json_structure_option))
                df_to_merge = load_json_data(new_json_structure_option, new_df_to_merge_file_name)
                frames = [current_data_frame, df_to_merge]
                new_df = pd.concat(frames, ignore_index=True, sort=False)
                no_of_files = no_of_files + 1
                print('Total no. of data loaded: ' + str(no_of_files))
                print('Details of the new data:')
                print(new_df.info())
                print(new_df.head())
            # The initial DataFrame i.e. current_data_frame is put in new_df variable if there is no merge.
            else:
                new_df = current_data_frame
                merge_data_menu = False  # Exit the outer while loop.
        loaded_data = new_df  # The produced data frame is accessed in the function body and assigned to be returned.
        input('JSON data now successfully loaded into memory. Press enter to proceed\n')
        return loaded_data

    # sub-functions mongodb loading.
    def mongodb_details():
        """
        This sub-function will help get mongodb connection details.

        Input:
            Users will be asked questions to determine this.

        Returns:
            A dictionary containing the connection details. port 'int', server name, collection, and db are 'strings'
        """
        print('Please ensure you provide accurate answers to the following question & press enter to move to the next.')
        mongodb_port_msg = '\nPlease enter MongoDB port no. It is usually \'27017\' by default:\n'
        mongodb_port_no = int(input(mongodb_port_msg))
        mongodb_server_msg = '\nPlease enter MongoDB Server. It is usually \'localhost\' by default:\n'
        mongodb_server_name = input(mongodb_server_msg)
        mongodb_db_msg = '\nPlease enter MongoDB Database. Where the data is located.\n'
        mongodb_db_name = input(mongodb_db_msg)
        mongodb_collection_msg = '\nPlease enter MongoDB Collection. The collection that contains the data.\n'
        mongodb_collection_name = input(mongodb_collection_msg)

        mongodb_connect_details = {'MONGODB_SERVER_NAME': mongodb_server_name,
                                   'MONGODB_DB_NAME': mongodb_db_name,
                                   'MONGODB_COLLECTION_NAME': mongodb_collection_name,
                                   'MONGODB_PORT_NO': mongodb_port_no}
        return mongodb_connect_details

    def mongodb_data_loader(connection_details):
        server_name = connection_details['MONGODB_SERVER_NAME']
        port_no = connection_details['MONGODB_PORT_NO']
        database_name = connection_details['MONGODB_DB_NAME']
        collection_name = connection_details['MONGODB_COLLECTION_NAME']
        print(server_name)
        print(port_no)
        print(database_name)
        print(collection_name)
        client = MongoClient(server_name, port_no)
        db = client[database_name]
        collection = db[collection_name]
        data_from_db = pd.DataFrame(list(collection.find()))
        return data_from_db

    def mongodb_handler():
        welcome= greet()
        connection_details = mongodb_details()
        data_from_db = mongodb_data_loader(connection_details)
        input('MongoDB data is successfully loaded into memory. Press enter to continue.\n')
        no_of_collection = 1
        print('Total no. of collections loaded: ' + str(no_of_collection))
        print('Details of the current data:')
        print(data_from_db.info())
        print(data_from_db.head())
        #  Add another data i.e merge row wise.
        print('\nData Merging Process')
        merge_data_menu = True  # Show the menu for the first time alert.
        while merge_data_menu:
            add_another_data_option = add_another_data_handler()
            # A new DataFrame will be formed if user chooses to add another data i.e. merge.
            if add_another_data_option == 1:
                con_details_msg = ('Do you want to use the previous database and connection details?\n'
                                ' Please select an option [1 or 2] \n'
                                ' [1] Yes- To use previous: port, database and server \n'
                                ' [2] No- I will provide new details.\n')
                # Preventing wrong input with try except block.
                display_con_details_msg = True
                while display_con_details_msg:
                    try:
                        use_previous_connection = int(input(con_details_msg))
                    except ValueError:
                        print("Whoops! That's not a number.")
                        continue
                    else:
                        print("Ok.")
                        display_con_details_msg = False
                if use_previous_connection == 1:
                    # Get previous connection details and update it.
                    current_connection_details = connection_details
                    new_collection_name = input('Enter the new collection name:\n')
                    # Update the value of the collection in the dictionary
                    current_connection_details['MONGODB_COLLECTION_NAME'] = new_collection_name
                else:
                    # call the connection manager to ask for an entirely new details.
                    current_connection_details = mongodb_details()
                # Then use the current connection details to load the additional data set
                new_df = mongodb_data_loader(current_connection_details)
                frames = [data_from_db, new_df]
                new_df = pd.concat(frames, ignore_index=True, sort=False)
                no_of_collection = no_of_collection + 1
                print('Total no. of data loaded: ' + str(no_of_collection))
                print('Details of the new data:')
                print(new_df.info())
                print(new_df.head())
            else:
                new_df = data_from_db
                merge_data_menu = False  # Exit the outer while loop.
        loaded_data = new_df  # The produced data frame is accessed in the function body and assigned to be returned.
        input('MongoDB collection data now successfully loaded into memory. Press enter to proceed\n')
        return loaded_data

    # when data_loader() is called, a simple text-based menu system to interact with the function starts here.
    def greet():
        welcome_msg = ('Welcome to the Data Pre-processor of the LMI System. '
                       'We will begin by loading the data sets into memory.')
        print(welcome_msg)
        time.sleep(0.20)
        # time.sleep(0.20) is a Jupyter Notebook Print-vs-Input Workaround. In case Jupyter notebook is used
        # This will prevent the input statement below from executing before the print.
        # https://stackoverflow.com/questions/50439035/jupyter-notebook-input-line-executed-before-print-statement

    main_menu_msg = ('\nPlease select an option [1, 2 or 0 to exit]: '
                     '\n [1] To Import JSON data format.'
                     '\n [2] To Import from MongoDB.'
                     '\n [0] To Exit.'
                     '\n ')
    # We need to catch an exception in case user enters a non integer.
    data_loader_menu_msg_display = True
    while data_loader_menu_msg_display:
        try:
            main_menu_option = int(input(main_menu_msg))
        except ValueError:
            print("Whoops! That's not a number.")
            continue
        else:
            print("Ok.")
            data_loader_menu_msg_display = False
    # We are now sure we are getting an integer. Call the right handler.
    if main_menu_option == 1:
        data = json_file_handler()
    else:
        data = mongodb_handler()
    return data


