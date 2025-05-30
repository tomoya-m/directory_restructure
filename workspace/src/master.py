import os

from dotenv import load_dotenv
import pandas
import psycopg2


def get_master(
    table_id: int
    , env_file_path: str = "/Workspace/Users/tomoya-m@aicello.co.jp/20250529_ディレクトリー構成変更/.env"
) -> pandas.DataFrame:
    """
    マスターDBの情報を取得する。

    Parameters
    ----------
    table_id : int
        テーブルID
    env_file_path : str, optional
        `.env`ファイルのパス, by default "/Workspace/Users/tomoya-m@aicello.co.jp/20250529_ディレクトリー構成変更/.env"

    Returns
    -------
    pandas.DataFrame
        マスターDBのデータ
    """
    # .env ファイルから環境変数を設定する。
    load_dotenv(dotenv_path=env_file_path)

    # マスターDBの接続情報を設定する。
    host = os.environ["MASTER_HOST"]
    database_name = os.environ["MASTER_DATABASE_NAME"]
    user_name = os.environ["MASTER_USER_NAME"]
    password = os.environ["MASTER_PASSWORD"]

    # マスターDBに接続する。
    connection = psycopg2.connect(f"host={host} dbname={database_name} user={user_name} password={password}")
    cursor = connection.cursor()

    # SELECTクエリを実行する。
    cursor.execute(
        f"""
        SELECT
            column_uuid as uuid,
            logical_name,
            physical_name,
            is_latest
        FROM columns
        WHERE table_id={table_id}
        """
    )

    # クエリの実行結果を`pandas.DataFrame`型で読み取る。
    query_result = cursor.fetchall()
    master_df = pandas.DataFrame(query_result, columns=[desc[0] for desc in cursor.description])

    return master_df


if __name__ == "__main__":
    master_df = get_master(table_id=6121)
    print(master_df)
