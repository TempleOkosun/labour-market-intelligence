import tkinter as tk  # Tkinter is used so that file import/export is simplified and chances of errors minimized.
from tkinter.filedialog import askopenfilename
import pandas as pd
import time
from pymongo import MongoClient


# Loading and merging data into a pandas data frame accessible in the memory.
def data_loader():
    """
    DOCSTRING: This function gets scraped data from the user and loads them into memory for pre-processing.
    Users will be prompted to choose scraped data file & structure till all data are loaded & merged row wise in memory.
    For JSON, get the filename and file structure and give to the json_file handler to load.
    For MongoDB, get the port and hostname and hand to the mongodb_handler to load.
    """

    # sub-functions for JSON loading
    def json_file_path():
        """
        DOCSTRING: This sub-function will open a dialog box to get JSON file path.
        OUTPUT: Exact file path is returned to be used by main loader function.
        """
        input('Press Enter to browse to the file location:\n')  # Pause to ensure user is in control.
        root = tk.Tk()
        root.withdraw()  # Hides the root frame. Else, a frame will be hanging around.
        root.update()  # may be necessary to force the hiding of the main frame.
        file_name = askopenfilename()
        root.destroy()
        return file_name

    def json_structure():
        """
        DOCSTRING: This sub-function will help determine JSON file structure i.e. how JSON documents are
        separated within the file so that the file can be read the file properly.
        Users will be asked questions to determine this.
        OUTPUT: An integer stating the structure of the JSON file is returned to main function.
        """
        json_structure_msg = ('Please select an option [1 or 2]' +
                              ' to indicate how JSON Objects were organized in the file:\n' +
                              '  [1] An array of JSON objects seperated by a comma \',\' as shown below:\n' +
                              '    [{"Job_Id":"15","Location":"Aberdeen"}, {"Job_Id":"67","Location":"Dundee"}] \n\n' +
                              '  [2] One JSON object per line usually when JSON was exported from MongoDB as shown below:\n' +
                              '    NB-Anything within{} is seen as a single line even if the text spans multiple lines.\n' +
                              '    {"Job_Id":"11","Location":"Cork"}\n' +
                              '    {"Job_Id":"30","Location":"England"}\n')

        json_structure_option = int(input(json_structure_msg))
        return json_structure_option

    def json_file_handler():
        def load_data(json_structure_option, json_file_name):
            """
            DOCSTRING: This function will load the JSON file into memory using the appropriate loader based on
            the JSON file structure option and file path.
            OUTPUT: A data frame is returned and now loaded to memory for manipulations.
            """
            if json_structure_option == 1:
                df = pd.read_json(path_or_buf=json_file_name, lines=False)
                return df

            elif json_structure_option == 2:
                df = pd.read_json(path_or_buf=json_file_name, lines=True)
                return df


        """
        DOCSTRING: This function handles the loading of json data format.
        Users will be prompted to choose scraped data location.
        """

        msg = '\nLoad JSON Documents.'
        print(msg)
        time.sleep(0.20)
        json_file_name = json_file_path()
        json_structure_option = json_structure()
        print('The selected file is: ' + json_file_name)
        print('The JSON file structure type is: ' + str(json_structure_option))
        current_data_frame = load_data(json_structure_option, json_file_name)
        print('Details of the current data:')
        print(current_data_frame.info())
        n = 1
        print('Total no. of data loaded: ' + str(n))

        #  Add another data i.e Merge rowise.
        print('\nData Merging Process')
        merge_data_menu = True  # Show the menu for the first time alert.
        while merge_data_menu:
            current_data_frame = current_data_frame
            n = n
            merge_data_msg = ('Would you like to add another data i.e. merge Rowise ?\n' +
                              ' Please select an option [1 0r 2] \n' +
                              ' [1] Yes- To select the data \n' +
                              ' [2] No- To proceed with data preparation.\n')
            merge_data_option = int(input(merge_data_msg))
            if merge_data_option == 1:
                new_df_to_merge_file_name = json_file_path()
                new_json_structure_option = json_structure()
                print('The selected file is: ' + new_df_to_merge_file_name)
                print('The JSON file structure is: ' + str(json_structure_option))
                df_to_merge = load_data(new_json_structure_option, new_df_to_merge_file_name)
                frames = [df_to_merge, current_data_frame]
                new_df = pd.concat(frames, ignore_index=True)
                print('Details of the new data:')
                print(new_df.info())
                n = n + 1
                print('Total no. of data loaded: ' + str(n) + '\n')

            else:
                input('Press enter to proceed.')
                new_df = current_data_frame
                merge_data_menu = False

        input('JSON data now successfully loaded into memory.')

    # sub-functions mongodb loading.
    def mongodb_details():
        """
        DOCSTRING: This sub-function will help get mongodb connection details.
        Users will be asked questions to determine this.
        """
        print('Please ensure you provide accurate answers to the following question & press enter to move to the next.')
        mongodb_port_msg = '\nPlease enter MongoDB port no. It is usually \'2701\' by default:\n'
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

    def mongodb_handler():
        connection_details = mongodb_details()

        server_name = connection_details['MONGODB_SERVER_NAME']
        port_no = connection_details['MONGODB_PORT_NO']
        database_name = connection_details['MONGODB_DB_NAME']
        collection_name = connection_details['MONGODB_COLLECTION_NAME']

        print(server_name)
        print('')
        print(port_no)
        print('')
        print(database_name)
        print('')
        print(collection_name)
        print('')


        client = MongoClient(server_name, port_no)
        db = client[database_name]
        collection = db[collection_name]
        data = pd.DataFrame(list(collection.find()))
        input('MongoDB data now successfully loaded into memory. Press enter to continue.')

    def greet():
        welcome_msg = ('Welcome to the Data Pre-processor of the LMI System. ' +
                       'We will begin by loading the data sets into memory.')
        print(welcome_msg)
    greet()
    time.sleep(0.20)
    # time.sleep(0.20) is a Jupyter Notebook Print-vs-Input Workaround. In case Jupyter notebook is used
    # This will prevent the input statement below from executing before the print.
    # https://stackoverflow.com/questions/50439035/jupyter-notebook-input-line-executed-before-print-statement

    main_menu_msg = ('\nPlease select an option [1, 2 or 0 to exit]: ' +
                     '\n [1] To Import JSON data format.' +
                     '\n [2] To Import from MongoDB.' +
                     '\n [0] To Exit.' +
                     '\n ')
    # We need to catch an exception when user enters a non integer.
    try:
        main_menu_option = int(input(main_menu_msg))
        if main_menu_option == 1:
            json_file_handler()

        elif main_menu_option == 2:
            mongodb_handler()

    except ValueError:
        print('Input Error')


if __name__ == '__main__':
    data_loader()


