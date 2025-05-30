import csv
import datetime
import os
from io import StringIO

from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
import pandas


class DirectoryRestructure:
    def __init__(
        self
        , device_id: str
        , adp_datalake_input_container_name: str
        , target_date: datetime.date
        , master_df: pandas.DataFrame
        , env_file_path: str = "/Workspace/Users/tomoya-m@aicello.co.jp/20250529_ディレクトリー構成変更/.env"
    ) -> None:
        """
        init

        Parameters
        ----------
        device_id : str
            デバイスID
        adp_datalake_input_container_name : str
            移行前のコンテナー名
        target_date : datetime.date
            対象日
        master_df : pandas.DataFrame
            マスター
        env_file_path : str, optional
            `.env`ファイルのパス, by default "/Workspace/Users/tomoya-m@aicello.co.jp/20250529_ディレクトリー構成変更/.env"
        """
        # .env ファイルから環境変数を設定する。
        load_dotenv(dotenv_path=env_file_path)

        # インスタンス変数を設定する。
        self.device_id = device_id
        self.adp_datalake_connection_string = os.environ["ADP_DATALAKE_CONNECTION_STRING"]
        self.adp_datalake_account_name = "dlsaicellodatalake001"
        self.adp_datalake_input_containers_name = adp_datalake_input_container_name
        self.adp_datalake_output_container_name = "scada-archive"
        self.target_year = str(target_date.year).zfill(4)
        self.target_month = str(target_date.month).zfill(2)
        self.target_day = str(target_date.day).zfill(2)
        self.master_df = master_df

        datalake_service_client = DataLakeServiceClient.from_connection_string(self.adp_datalake_connection_string)
        self.input_file_system_client = datalake_service_client.get_file_system_client(self.adp_datalake_input_containers_name)
        self.output_file_system_client = datalake_service_client.get_file_system_client(self.adp_datalake_output_container_name)

        return


    def main(self) -> None:
        """
        main

        1. 移行前のコンテナーから、対象日のCSVファイルを読み取る。
        2. ヘッダーにあるアイテム名を最新の論理名に変更する。
        3. 移行後のコンテナーにCSVの内容を書き込む。
        """
        data = self.read_csv()
        prepped_data = self.rename_header(data=data)
        self.write_csv(data=prepped_data)
        return


    def read_csv(self) -> list:
        """
        移行前のコンテナーから、CSVファイルを読み取る。

        Returns
        -------
        list
            読み取ったCSVファイルの中身
        """
        # 入力ファイルパスを設定する。
        input_path = f"year={self.target_year}/month={self.target_month}/{self.target_year}{self.target_month}{self.target_day}.csv"

        # ファイルクライアントを取得する。
        input_file_client = self.input_file_system_client.get_file_client(input_path)

        # 入力ファイルを読み込む。
        download = input_file_client.download_file()
        content = download.readall().decode("utf-8")

        reader = csv.reader(StringIO(content))

        return list(reader)


    def rename_header(self, data: list) -> list:
        """
        ヘッダーにあるアイテム名を、最新の論理名に変換する。

        Parameters
        ----------
        data : list
            読み取ったCSVファイルの中身

        Returns
        -------
        list
            ヘッダー変換後のCSVファイルの中身
        """
        original_header = data[0]
        new_header = []

        for col in original_header:
            # 該当するUUIDを探す。
            matched_rows = self.master_df[
                (self.master_df["physical_name"]==col) | (self.master_df["logical_name"]==col)
            ]

            if not matched_rows.empty:
                # UUIDを取得する。
                matched_uuid = matched_rows.iloc[0]["uuid"]

                # UUIDに対応する`is_latest == True`の行を探す。
                latest_row = self.master_df[
                    (self.master_df["uuid"]==matched_uuid) & (self.master_df["is_latest"]==True)
                ]

                # `is_latest == True`の行の論理名を参照する。
                if not latest_row.empty:
                    new_name = latest_row.iloc[0]["logical_name"]
                    new_header.append(new_name)
                # `is_latest == True`の行がない場合は、エラーにする。
                # マスターDB側に不備がある。
                else:
                    raise ValueError(f"{col=}で名称の変換が正しくできません。")

            else:
                raise ValueError(f"{col=}に対応するUUIDが見つかりませんでした。")

        data[0] = new_header

        return data


    def write_csv(self, data: list):
        """
        移行後のコンテナーに、CSVファイルを書き込む。

        Parameters
        ----------
        data : list
            書き込むCSVファイルの中身
        """
        # 出力ファイルパスを設定する。
        output_path = f"device_id={self.device_id}/year={self.target_year}/month={self.target_month}/{self.target_year}{self.target_month}{self.target_day}.csv"

        # ファイルクライアントを取得する。
        output_file_client = self.output_file_system_client.get_file_client(output_path)

        # メモリに書き込む。
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerows(data)
        buffer.seek(0)

        # BOM付きUTF-8でエンコードする。
        encoded_data = buffer.getvalue().encode("utf-8-sig")

        # ファイルを書き込む。
        output_file_client.upload_data(
            data=encoded_data
            , overwrite=True
        )

        buffer.close()

        return
