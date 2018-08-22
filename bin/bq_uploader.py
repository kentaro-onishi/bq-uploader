#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#title           :bq_uploader.py
#usage           :python bq_uploader.py
#python_version  :3.5.3
#==============================================================================
import sys
import os
from configparser import ConfigParser
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import Dataset
import logging
import datetime
import ast
import importlib
import io
import re

# 親ディレクトリをアプリケーションのホーム(${app_home})に設定
app_home = os.path.abspath(os.path.join( os.path.dirname(os.path.abspath(__file__)) , ".." ))
# ${app_home}/libをライブラリロードパスに追加
# ${app_home}/schemaをライブラリロードパスに追加
sys.path.append(os.path.join(app_home,"lib"))
sys.path.append(os.path.join(app_home,"schema"))

# メール送信用ライブラリをインポート
from mail import Mailer

if __name__ == "__main__" :
    # 自身の名前から拡張子を除いてプログラム名(${prog_name})にする
    prog_name = os.path.splitext(os.path.basename(__file__))[0]

    # 設定ファイルを読む
    config = ConfigParser()
    conf_path = os.path.join(app_home,"conf", prog_name + ".conf")
    config.read(conf_path)

    # 引数チェック
    args = sys.argv
    if len(args) == 2 and re.match(r"^\d{8}$", args[1]):
        # 引数指定の場合は、スキーマ名、ログファイル、GS上のファイル名は引数の日付
        table_date = args[1]
        date = args[1]
    else:
        # スキーマ名の日付、GS上のファイル名は１日前を対象にする
        table_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
        # ログファイルは当日の日付
        date = datetime.datetime.now().strftime("%Y%m%d")

    # ロガーの設定
    message = ''
    # フォーマット
    log_format = logging.Formatter("%(asctime)s [%(levelname)8s] %(message)s")
    # レベル
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # ログファイルへのハンドラ
    file_handler = logging.FileHandler(os.path.join(app_home,"log", prog_name + date + ".log"), "a+")
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    # 変数へのハンドラ
    log_message = io.StringIO()
    stream_handler = logging.StreamHandler(log_message)
    stream_handler.setFormatter(log_format)
    logger.addHandler(stream_handler)

    bq_client = bigquery.Client(project=config.get("base","project_id"))
    st_client = storage.Client(project=config.get("base","project_id"))
    dataset = bq_client.dataset(config.get("base","dataset"))

    # 処理開始
    try:
        # ログ出力
        logger.info("start")
        target = ast.literal_eval(config.get('base','target'))
        tables = list(bq_client.list_dataset_tables(dataset))
        table_ids = []
        for table in tables:
            table_ids.append(table.table_id)
        for key, schema in sorted(target.items()):
            logger.info('{} インポート開始'.format(key))
            # モジュールのロード
            exec("from {} import {}".format(key, schema))
            # クラスのインスタンス化
            ins = eval("{}(bq_client, st_client, dataset, config.get('base','bucket'), table_date)".format(schema))
            exists_table = ins.get_table() in table_ids
            # データロード処理
            result = ins.load_data(exists_table)
            if not result:
                logger.warning('{} : cloud storage にインポート対象のファイルがありません'.format(key))
            else:
                result.result()
                if result.state == 'DONE':
                    logger.info('{} インポート完了'.format(key))
    except Exception as e:
        # キャッチして例外をログに
        logger.exception(e)
        logger.info('exit')
        mail = Mailer('[bq-uploader]エラー発生', 'CSVインポートでエラーが発生しました\n' + e.message + '\n\n処理結果ログ\n\n' + log_message.getvalue())
        mail.send_message()
        sys.exit(1)

    logger.info('exit')
    mail = Mailer('[bq-uploader]処理完了', 'CSVインポートが完了しました。\n\n処理結果ログ\n\n' + log_message.getvalue())
    mail.send_message()
