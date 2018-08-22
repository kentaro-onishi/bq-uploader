# environment
GCPの同じプロジェクトID内にてVMを作製する。
選択するVMのスペックは、一番低い物で十分です。
OSは、Debian９を選択する

注意）
インスタンスつくる際、Cloud API アクセス スコープのBigQueryを有効にする

# setting
```
# rootへsudoする
$ sudo su -
# pythonインストール
$ apt update
$ apt install python3 python3-dev git gcc make -y
$ wget https://bootstrap.pypa.io/get-pip.py
$ python3 get-pip.py
$ pip3 install --upgrade google-cloud

# 時刻設定
$ echo "Asia/Tokyo" | tee /etc/timezone
$ dpkg-reconfigure tzdata
## 表示するされた画面より Asian -> Tokyoを選択する

# 設定ファイル変更
$ cd bq-uploader
$ vi conf/bq_uploader.conf
## project_id = {プロジェクトID}
## bucket = {GoogleCloudStorageのバケット名}
## dataset = {GoogleBigQueryのデータセット名}

# メール送信先を変更
$ vi lib/mail.py
## TO_ADDRESS = '{Toメール送り先}'
## CC_ADDRESS = '{Ccメール送り先}'
```

## usage
コマンドの引数に日付（YYYYMMDDの形式）を指定したら、cloud storage上の指定の日付のCSVをインポートします
```
$ /usr/bin/python3 /root/bq-uploader/bin/bq_uploader.py {日付指定（省略可）}
```

## log
コマンドのログは以下のディレクトリに処理実行日ごとに出力します。
```
/root/bq-uploader/log
```