import uuid
import re
from selectdb_utils import CopySQLBuilder
from error import *
import time
from http_builder import *
from batch import *

COMMIT_PATTERN = "http://%s/copy/query"
NULL_VALUE = "\\N"


class SelectDBStageLoad:
    SUCCESS = 0
    FAIL = "1"
    COMMITTED_PATTERN = re.compile(
        r"errCode = 2, detailMessage = No files can be copied, matched (\d+) files, filtered (\d+) files because files may be loading or loaded")

    def __init__(self, settings):
        self.host_port = f"{settings.fe_host}:{settings.fe_http_port}"
        self.user = settings.username
        self.passwd = settings.password
        self.load_url_str = f"http://{self.host_port}/copy/upload"
        self.commit_url_str = f"http://{self.host_port}/copy/query"
        self.file_split_size = settings.file_split_size
        self.row_batch_size = settings.row_batch_size
        self.cluster_name = settings.cluster_name
        self.copy_into_props = settings.get_copy_into_props()
        # self.df_columns = settings.columns
        self.session = requests.Session()

    def get_load_url_str(self):
        return self.load_url_str

    def load(self, value, db, table):
        file_name = str(uuid.uuid4())
        address = self.get_upload_address(file_name)
        self.upload_file(address, value, file_name)
        self.execute_copy(file_name, db, table)

    def gen_file_name(self, suffix: int):
        file_name = f"{str(uuid.uuid4())}_%s"
        return file_name % suffix

    def load_list(self, rows: list[list], db, table, columns: list = None):
        file_name = f"{str(uuid.uuid4())}_%s"
        file_num = 0
        if self.copy_into_props['file.type'] == "csv":
            line_delimiter = self.copy_into_props["file.line_delimiter"]
            field_delimiter = self.copy_into_props["file.column_separator"]
            batch = CsvBatch(field_delimiter, line_delimiter)
            for i, row in enumerate(rows):
                batch.add_line(row)
                if batch.get_capacity() > self.file_split_size or len(batch.batch_data) >= self.row_batch_size:
                    upload_file_name = file_name % i
                    address = self.get_upload_address(upload_file_name)
                    self.upload_file(address, batch.get_data(), upload_file_name)
                    batch.clear_batch()

            if batch.get_capacity() > 0:
                upload_file_name = file_name % file_num
                file_num += 1
                address = self.get_upload_address(upload_file_name)
                self.upload_file(address, batch.get_data(), upload_file_name)

            batch.clear_batch()
            self.execute_copy(file_name, db, table)
        else:
            json_batch = JsonBatch(columns)
            try:
                for i, row in enumerate(rows):
                    json_batch.add_data(row)
                    if (json_batch.get_capacity() >= self.file_split_size or
                            len(json_batch.batch_data) >= self.row_batch_size):
                        upload_file_name = file_name % file_num
                        file_num += 1
                        address = self.get_upload_address(upload_file_name)
                        # print(address)
                        data = json_batch.get_data()
                        self.upload_file(address, data, upload_file_name)
                        print(upload_file_name)
                        json_batch.clear_batch()

                if json_batch.get_capacity() > 0:
                    upload_file_name = file_name % file_num
                    address = self.get_upload_address(upload_file_name)
                    # print(address)
                    upload_data = json_batch.get_data()
                    self.upload_file(address, upload_data, upload_file_name)
                json_batch.clear_batch()
                self.execute_copy(file_name, db, table)
            except Exception as e:
                raise RuntimeError(e)

    def execute_copy(self, file_name, db, table):
        start = int(time.perf_counter() * 1000)
        copy_sql_builder = CopySQLBuilder(self.copy_into_props, f"{db}.{table}", file_name)
        copy_sql = copy_sql_builder.build_copy_sql()
        print(f"build copy SQL is {copy_sql}")
        params = {"sql": copy_sql}
        if self.cluster_name:
            params["cluster"] = self.cluster_name

        post_builder = HttpPostBuilder()
        post_builder.set_url(f"{self.commit_url_str}").base_auth(self.user, self.passwd).set_entity(
            json.dumps(params))

        file_name = file_name.replace("%s", "*")

        with requests.Session() as session:
            request = session.prepare_request(post_builder.build())
            response = session.send(request)
            status_code = response.status_code
            reason_phrase = response.reason

            if status_code != 200:
                print(f"commit failed with status {status_code} {self.host_port}, reason {reason_phrase}")
                raise SelectdbException(f"commit error with file: {file_name}")
            elif response.content:
                load_result = response.content.decode("utf-8")
                success = self.handle_commit_response(load_result)
                if success:
                    print(f"commit success cost {int(time.time() * 1000) - start}ms, response is {load_result}")
                else:
                    raise SelectdbException(f"commit failed with file: {file_name}")

    def handle_commit_response(self, load_result):
        base_response = json.loads(load_result)
        if base_response["code"] == SelectDBStageLoad.SUCCESS:
            data_resp = base_response["data"]
            if base_response["code"] == SelectDBStageLoad.FAIL:
                print(f"Copy into execute failed, reason: {load_result}")
                return False
            else:
                result = data_resp["result"]
                if result["state"] != "FINISHED" and not self.is_committed(result["msg"]):
                    print(f"Copy into load failed, reason: {load_result}")
                    return False
                else:
                    print(result)
                    return True
        else:
            print(f"Commit failed, reason: {load_result}")
            return False

    def is_committed(self, msg):
        return SelectDBStageLoad.COMMITTED_PATTERN.match(msg) is not None

    def upload_file(self, address, value, file_name):
        put_builder = HttpPutBuilder()
        put_builder.set_url(address).add_common_header().set_entity(value.encode("utf-8"))

        try:
            with requests.Session() as session:
                request = session.prepare_request(put_builder.build())
                response = session.send(request)
                if response.status_code != 200:
                    result = response.content.decode("utf-8") if response.content else None
                    print(f"Upload file {file_name} error, response {result}")
                    raise SelectdbException(f"Upload file error: {file_name}")
        except SelectdbException as e:
            raise RuntimeError(e)

    def get_upload_address(self, file_name, prepared_reques=None):
        put_builder = HttpPutBuilder()
        put_builder.set_url(self.load_url_str).add_file_name(
            file_name).add_common_header().set_empty_entity().base_auth(self.user, self.passwd)

        try:
            with requests.Session() as session:
                request = session.prepare_request(put_builder.build())

                response = session.send(request, allow_redirects=False)
                status_code = response.status_code
                reason = response.reason
                print(response.headers)
                if status_code == 307:
                    location = response.headers.get("location")
                    upload_address = location
                    print(f"redirect to s3: {upload_address}")
                    return upload_address
                else:
                    result = response.text
                    print(
                        f"Failed to get internalStage address, status {status_code}, reason {reason}, response {result}")
                    raise SelectdbException("Failed get internalStage address")
        except SelectdbException as e:
            raise RuntimeError(e)
