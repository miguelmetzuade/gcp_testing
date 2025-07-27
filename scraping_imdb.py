import requests
from io import BytesIO
from gzip import GzipFile
from pandas import DataFrame
from datetime import datetime, timedelta, timezone


PROCESS_IMDB_SCRAPING_DAILY = 'process_imdb_scraping_daily'

def get_current_time() -> str: return datetime.now(timezone.utc).isoformat(timespec='seconds')

def get_current_date() -> str: return datetime.now(timezone.utc).date().isoformat()

def get_current_week() -> int: return datetime.now(timezone.utc).isocalendar()[1]

def get_current_year() -> int:
    now = datetime.now(timezone.utc)
    while now.weekday() != 0:
        now -= timedelta(days=1)
    return now.isocalendar()[0]


class ScrapingImdb:
    """
    Clase para realizar el scraping de datos desde IMDb.
    """

    __DATE = get_current_date()
    __CURRENT_YEAR = get_current_year()
    __WEEK = get_current_week()
    __TIMESTAMP = get_current_time()

    __SOURCE = 'imdb'

    __URL_FILE = 'https://datasets.imdbws.com/title.ratings.tsv.gz'

    def __init__(self) -> None:

        self.__PROCESS_NAME = PROCESS_IMDB_SCRAPING_DAILY

    
    def process(self) -> list:
        try:
            start = datetime.now()
            df = self.__process()
            end = datetime.now()
            print(f'Tiempo transcurrido del proceso: {end - start}.')
            return df
        except Exception as e:
            raise
        finally:
            pass


    def __process(self) -> None:
        """
        Método privado que realiza el proceso principal de scraping y procesamiento de datos desde IMDb.

        Returns:
            None
        """

        print(f'Inicia el proceso de {self.__PROCESS_NAME}.')

        data_imdb = self.__download_source_imdb()
        if not data_imdb:
            raise Exception(
                '¡No se pudo descargar y descomprimir el archivo de IMDb!')

        df_data = self.__generate_df_export(data_imdb=data_imdb)

        self.__complete_df_fields(df=df_data)

        self.__delete_repeats_by_imdb(df=df_data)

        # self.__export_test(df=df_data)

        print(f'Finaliza el proceso de {self.__PROCESS_NAME}.')
        return df_data

    def __download_source_imdb(self) -> str:
        """
        Descarga y descomprime el archivo de datos desde la fuente IMDb.

        Returns:
            str or None: Los datos descomprimidos como una cadena de texto o None si la descarga falla.
        """

        print(
            f'Descargando y descomprimiendo el archivo de {self.__URL_FILE}.')

        attemps = 0
        while attemps < 5:
            try:
                response = requests.get(self.__URL_FILE)
                response.raise_for_status()
                compressed_data = response.content

                with GzipFile(fileobj=BytesIO(compressed_data), mode='rb') as decompressed_file:
                    data = decompressed_file.read()
                    return data.decode('utf-8')

            except requests.exceptions.RequestException as e:
                print(f'Error al realizar la solicitud: {e}.')
                attemps += 1
                print(f'Intento número: {attemps}.')
        return None

    def __generate_df_export(self, data_imdb: str) -> DataFrame:
        """
        Crea un DataFrame a partir de los datos de IMDb.

        Args:
            data_imdb (str): Los datos de IMDb en formato de cadena de texto.

        Returns:
            DataFrame: El DataFrame con los datos de IMDb.
        """

        print(f'Creando DF con datos de IMDb...')

        rows = data_imdb.strip().split('\n')
        data_imdb = [row.split('\t') for row in rows]
        data_imdb.pop(0)
        df = DataFrame(data=data_imdb, columns=[self.__SOURCE, 'rating', 'votes'])
        df = df.astype(dtype={'rating': 'float64', 'votes': 'float64'})
        return df

    def __complete_df_fields(self, df: DataFrame) -> None:
        """
        Completa los campos del DataFrame.

        Args:
            df (DataFrame): El Dataframe final.

        Returns:
            None
        """

        df['date'] = self.__DATE
        df['timestamp'] = self.__TIMESTAMP
        df['week'] = self.__WEEK
        df['currentYear'] = self.__CURRENT_YEAR
        df['source'] = self.__SOURCE

    def __delete_repeats_by_imdb(self, df: DataFrame) -> None:
        """
        Elimina los elementos repetidos por IMDb, dejando solamente uno.

        Args:
            df (DataFrame): El Dataframe final.

        Returns:
            None
        """

        print(f'Eliminado repetidos por IMDb en el DF...')

        df.drop_duplicates(subset='imdb', keep='first', inplace=True)

