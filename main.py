import functions_framework
from pandas import DataFrame
from datetime import datetime, timezone

from scraping_imdb import ScrapingImdb
from google_drive_api import GoogleDriveAPI


def __upload_gsheet_testing(df: DataFrame) -> None:
    gdrive_api = GoogleDriveAPI()
    folder_id = '1t29Hfv5HBibnoDWD4KXBhnAgqB_J1kPB'
    current_date = datetime.now(timezone.utc).date().isoformat()
    file_name = f'imdb_scraping_testing_{current_date}'

    df_data_test = df.head()

    file_id = gdrive_api.create_gsheet_file_in_folder(folder_id=folder_id, filename=file_name)
    print(f"File creqated with ID: {file_id}")
    gdrive_api.update_gsheet_data(gsheet_id=file_id, df=df_data_test)#, sheet_name='testing_01')


def main():
    imdb_scrapper = ScrapingImdb()
    df_imdb = imdb_scrapper.process()

    __upload_gsheet_testing(df=df_imdb)

    data_json_imdb = df_imdb.head(3).to_dict('records')
    return data_json_imdb


@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    return_data = main()
    return return_data

    # if request_json and 'name' in request_json:
    #     name = request_json['name']
    # elif request_args and 'name' in request_args:
    #     name = request_args['name']
    # else:
    #     name = 'World'
    # return 'Hello {}!'.format(name)


# if __name__ == '__main__':
#     main()
