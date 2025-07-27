import os.path
from pandas import DataFrame

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


token_file = 'token.json'
credentials_file = 'client_secret.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive']

def check_credentials() -> Credentials:
    """ Check and refresh Google API credentials.

    Returns:
        Credentials: Validated Google API credentials.
    """
    credentials= None
    if os.path.exists(token_file):
        print(f"Loading credentials from {token_file}...")
        credentials = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not credentials:
        print(f"No credentials found in {token_file}.")
    if not credentials.valid:
        print("Credentials are not valid or expired. Refreshing or obtaining new credentials...")

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials._refresh_token:
            print("Refreshing expired credentials...")
            credentials.refresh(Request())
        else:
            print(f"Obtaining new credentials from {credentials_file}...")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            credentials = flow.run_local_server(port=0)
        with open(token_file, 'w') as token:
            print(f"Saving new credentials to {token_file}...")
            token.write(credentials.to_json())
    return credentials


LAST_DATE_COL_NAME = 'latest_execution_date'
IS_LAST_DATE_COL_NAME = 'is_last_execution_date'


class GoogleDriveAPI:

    def __init__(self):
        self.credentials = check_credentials()
        self.drive_service = build("drive", "v3", credentials=self.credentials)
        self.sheets_service = build("sheets", "v4", credentials=self.credentials)


    def create_gsheet_file_on_root(self, filename: str) -> str:
        """ Create a new Google Sheets file.

        Args:
            file_name (str): The name of the Google Sheets file.
        Returns:
            str: The ID of the created Google Sheets file.
        """
        spreadsheet_body = {
            'properties': {
                'title': filename
            }
        }
        response = self.sheets_service.spreadsheets().create(
            body=spreadsheet_body,
            fields='spreadsheetId'
        ).execute()
        file_id = response.get('spreadsheetId')
        print(f"Created Google Sheets file with ID: {file_id}")
        return file_id


    def move_file_to_folder(self, file_id: str, folder_id: str) -> None:
        """ Move a file to a specified folder.

        Args:
            file_id (str): The ID of the file.
            folder_id (str): The ID of the folder to move the file to.
        """
        file = self.drive_service.files().get(fileId=file_id, fields='id, parents').execute()
        current_parents = ",".join(file.get('parents', []))
        self.drive_service.files().update(
            fileId=file_id,
            addParents=folder_id,
            removeParents=current_parents, # 'root', # Remove from root if needed
            fields='id, parents'
        ).execute()
        print(f"Moved file {file_id} to folder {folder_id}.")


    def get_files_names_from_folder(self, folder_id: str) -> list:
        """ Get all files names and ids from a specific folder.

        Args:
            folder_id (str): The ID of the folder to search in.

        Returns:
            list: List of tuples, (file name, file IDs).
        """
        query = f"'{folder_id}' in parents and trashed=false and mimeType != 'application/vnd.google-apps.folder'"
        results = self.drive_service.files().list(
            q=query,
            fields="files(id, name)",
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        file_list = results.get('files', [])
        file_names = list()

        if not file_list:
            print(f"No files found in folder '{folder_id}' or folder does not exist/is not accessible with the provided credentials.")
            # raise Exception(f"No files found in folder '{folder_id}' or folder does not exist/is not accessible with the provided credentials.")

        else:
            for file in file_list:
                file_names.append((file['name'], file['id'])) # Use 'name' as per googleapiclient's file resource
                # print(f"Found file: {file['name']} (ID: {file['id']})")

        return file_names


    def get_file_id_by_name_in_folder(self, filename: str, folder_id: str) -> str:
        """ Get the file ID of a Google Sheets file by its name in a specific folder.

        Args:
            filename (str): The name of the Google Sheets file.
            folder_id (str): The ID of the folder to search in.

        Returns:
            str: The file ID if found, None otherwise.
        """
        query = f"'{folder_id}' in parents and name='{filename}' and trashed=false"

        results = self.drive_service.files().list(
            q=query,
            fields="files(id, name)",
            pageSize=1, # You only need to find one instance to confirm existence
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        file_list = results.get('files', None)

        if file_list and len(file_list) > 0:
            return file_list[0]['id']
        else:
            return None


    def read_gsheet_file_content(self, file_id: str, sheet_name: str='Sheet1', range: str=None) -> list:
        """ Read the content of a Google Sheets file.

        Args:
            file_id (str): The ID of the Google Sheets file.
            sheet_name (str): The name of the sheet to read from. Defaults to 'Sheet1'.
            range (str): The A1 notation of the sheet and range to retrieve data from.
                            Defaults to None (which reads the entire sheet).
                            Example: 'A1:C10'.

        Returns:
            list: A list of lists containing the content of the Google Sheets file.
        """
        print(f"Reading data from Google Sheet with ID: {file_id}, sheet name: '{sheet_name}', range: '{range}'...")
        sheet_range = f"{sheet_name}"
        if range:
            sheet_range = f"{sheet_name}!{range}"
        
        result = self.sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id,
            range=sheet_range
        ).execute()
        values = result.get('values', [])
        return values


    def read_gsheet_to_dataframe(self, file_id: str, sheet_name: str='Sheet1', range: str=None) -> DataFrame:
        """
        Reads data from a Google Sheet into a Pandas DataFrame.

        Args:
            file_id (str): The ID of the Google Sheet file.
            sheet_name (str): sheet name to read from. Defaults to 'Sheet1'.
            range (str): The A1 notation of the sheet and range to retrieve data from.
                            Defaults to None (which reads the entire sheet).
                            Example: 'A1:C10'.

        Returns:
            DataFrame: A Pandas DataFrame containing the data from the Google Sheet.
                        Returns an empty DataFrame if no data is found or an error occurs.
        """

        values = self.read_gsheet_file_content(file_id=file_id, sheet_name=sheet_name, range=range)

        if not values:
            print(f"No data found in Google Sheet '{file_id}' for sheet '{sheet_name}' and range '{range}'.")
            return DataFrame()

        # The first row is typically the header
        headers = values[0]
        data = values[1:] # All subsequent rows are data

        df = DataFrame(data, columns=headers)
        print(f"Successfully read {len(df)} rows into DataFrame from Google Sheet '{file_id}'.")
        return df


    def file_exists_on_folder(self, filename: str, folder_id: str) -> bool:
        """ Check if a Google Sheets file exists in a specific folder.

        Args:
            filename (str): The name of the Google Sheets file.
            folder_id (str): The ID of the folder to search in.

        Returns:
            bool: True if the file exists in the folder, False otherwise.
        """

        # # Possible solution: read all files in the folder and check if the filename matches any of them
        # files = __get_files_from_folder(folder_id)
        # for file_name, file_id in files:
        #     if file_name == filename:
        #         return True
        # return False

        query = f"'{folder_id}' in parents and name='{filename}' and trashed=false"

        results = self.drive_service.files().list(
            q=query,
            fields="files(id, name)",
            pageSize=1, # You only need to find one instance to confirm existence
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        file_list = results.get('files', None)
        # You can access file_list[0]['id'] and file_list[0]['name'] for details

        return file_list is not None and len(file_list) > 0


    def create_gsheet_file_in_folder(self, folder_id: str, filename: str) -> str:
        """
        Creates a new Google Sheet file with a specified name inside a Google Drive folder.

        Args:
            folder_id (str): The ID of the Google Drive folder where the sheet will be created.
            filename (str): The desired name of the new Google Sheet file.
        Returns:
            str: The ID of the newly created Google Sheet.
        """

        if self.file_exists_on_folder(folder_id=folder_id, filename=filename):
            print(f"File '{filename}' already exists in folder '{folder_id}'.")
            return self.get_file_id_by_name_in_folder(filename=filename, folder_id=folder_id)

        file_metadata = {
            'name': filename,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [folder_id]
        }
        
        print(f"Creating Google Sheet '{filename}' in folder '{folder_id}'...")
        file = self.drive_service.files().create(
            body=file_metadata,
            fields='id, webViewLink', # Request ID and URL
            supportsAllDrives=True # Important for Shared Drives
        ).execute()

        sheet_id = file.get('id')
        sheet_url = file.get('webViewLink')
        print(f"Google Sheet '{filename}' created with ID: {sheet_id} at: {sheet_url}")

        return sheet_id


    def __set_last_date_formula(self, gsheet_id: str, sheet_name: str, cols_number: int,
                                records_number: int, dag_execution_date_col_order: int=1) -> None:
        """Sets the last date formula in the specified Google Sheet.

        Args:
            gsheet_id (str): Google Sheets file ID.
            sheet_name (str): Name of the sheet to update.
            cols_number (int): Number of columns in the sheet.
            records_number (int): Number of records to update.
            dag_execution_date_col_order (int, optional): Column order for the DAG execution date. Defaults to 1.
        """
        sheet_cols_letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        sheet_col = sheet_cols_letters[dag_execution_date_col_order-1]  # Convert column order to letter
        formula_max_date = f"=MAX({sheet_col}2:{sheet_col})"
        data = [
            [formula_max_date] for i in range(records_number-1)
        ]
        data.insert(0, [LAST_DATE_COL_NAME]) # Insert header row
        body = {'values': data}

        sheet_range = f"{sheet_name}!{sheet_cols_letters[cols_number]}1"
        sheet = self.sheets_service.spreadsheets()

        result = sheet.values().update(
            spreadsheetId=gsheet_id,
            range=sheet_range,
            valueInputOption='USER_ENTERED', # RAW: as text; USER_ENTERED: interprets formulas/format
            body=body
        ).execute()

        print(f"{result.get('updatedCells')} updated cells with last date formula.")


    def update_gsheet_data(self, gsheet_id: str, df: DataFrame, sheet_name: str='Sheet1', range: str='A1',
                           dag_execution_date_col_order: int=1) -> None:
        """ Update a Google Sheets file.

        Args:
            gsheet_id (str): The ID of the Google Sheets file.
            df (DataFrame): DataFrame with data to be written to the Google Sheets file.
            sheet_name (str): The name of the sheet to update.
            range (str): The range in the sheet where the data will be written, e.g. 'A1:C10'.
        """

        existent_content = self.read_gsheet_file_content(file_id=gsheet_id, sheet_name=sheet_name, range=None)

        # sheet_cols_letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        # sheet_col_date = sheet_cols_letters[dag_execution_date_col_order-1]  # Convert column order to letter
        # formula_max_date = f"=MAX({sheet_col_date}2:{sheet_col_date})"
        # df[LAST_DATE_COL_NAME] = formula_max_date  # Add the last date column with the formula

        df.reset_index(drop=True, inplace=True)
        if existent_content and len(existent_content) > 0:
            df.index = df.index + 1 + len(existent_content) # Adjust index to append data after existing content
        else:
            df.index = df.index + 2 # Start from row 2 to account for header row

        # max_date_col = sheet_cols_letters[df.columns.size-1]
        # formula_is_last_date = f"={sheet_col_date}"+"{index_1}"+f"={max_date_col}"+"{index_2}" # f"=IF({sheet_col}2={LAST_DATE_COL_NAME}, TRUE, FALSE)"
        # df[IS_LAST_DATE_COL_NAME] = df.apply(
        #     lambda row: formula_is_last_date.format(index_1=row.name, index_2=row.name),
        #     axis=1
        # )

        cols_number = len(df.columns)

        data = df.values.tolist()  # Convert DataFrame to list of lists
        data.insert(0, df.columns.tolist())  # Insert header row

        if existent_content:
            existent_headers = existent_content[0]
            # existent_headers.remove(LAST_DATE_COL_NAME)  # Remove the last date column if it exists
            if existent_headers != df.columns.tolist():
                raise Exception(f"Headers in the DataFrame do not match the existing headers in the Google Sheet: {existent_headers} != {df.columns.tolist()}")
            data = data[1:]  # Skip the header row if it already exists
            range = f"A{len(existent_content) + 1}"

        sheet = self.sheets_service.spreadsheets()

        body = {'values': data}
        sheet_range = f"{sheet_name}!{range}"

        result = sheet.values().update(
            spreadsheetId=gsheet_id,
            range=sheet_range,
            valueInputOption='USER_ENTERED', # RAW: as text; USER_ENTERED: interprets formulas/format
            body=body
        ).execute()

        print(f"{result.get('updatedCells')} updated cells.")

        # self.__set_last_date_formula(gsheet_id=gsheet_id, sheet_name=sheet_name, cols_number=cols_number,
        #                              records_number=len(data)+len(existent_content),
        #                              dag_execution_date_col_order=dag_execution_date_col_order)


    def get_files_names_from_folder(self, folder_id: str) -> list:
        """ Get all files names and ids from a specific folder.

        Args:
            folder_id (str): The ID of the folder to search in.

        Returns:
            list: List of tuples, (file name, file IDs).
        """
        query = f"'{folder_id}' in parents and trashed=false and mimeType != 'application/vnd.google-apps.folder'"
        results = self.drive_service.files().list(
            q=query,
            fields="files(id, name)",
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        file_list = results.get('files', [])
        file_names = list()

        if not file_list:
            print(f"No files found in folder '{folder_id}' or folder does not exist/is not accessible with the provided credentials.")
            # raise Exception(f"No files found in folder '{folder_id}' or folder does not exist/is not accessible with the provided credentials.")

        else:
            for file in file_list:
                file_names.append((file['name'], file['id'])) # Use 'name' as per googleapiclient's file resource
                # print(f"Found file: {file['name']} (ID: {file['id']})")

        return file_names


    def get_file_id_by_name_in_folder(self, filename: str, folder_id: str) -> str:
        """ Get the file ID of a Google Sheets file by its name in a specific folder.

        Args:
            filename (str): The name of the Google Sheets file.
            folder_id (str): The ID of the folder to search in.

        Returns:
            str: The file ID if found, None otherwise.
        """
        query = f"'{folder_id}' in parents and name='{filename}' and trashed=false"

        results = self.drive_service.files().list(
            q=query,
            fields="files(id, name)",
            pageSize=1, # You only need to find one instance to confirm existence
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        file_list = results.get('files', None)

        if file_list and len(file_list) > 0:
            return file_list[0]['id']
        else:
            return None
