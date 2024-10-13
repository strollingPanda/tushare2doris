import json
import datetime


class CsvBatch:
    def __init__(self, field_delimiter: str, line_delimiter: str):
        self.batch_data = []
        self.capacity = 0
        self.field_delimiter = field_delimiter
        self.line_delimiter = line_delimiter

    def get_capacity(self):
        return self.capacity

    def add_line(self, row: list):
        line_data = self.field_delimiter.join(str(value) if value is not None else '\\N' for value in row)
        self.capacity += len(line_data.encode('utf-8'))
        self.batch_data.append(line_data)

    def get_data(self):
        return self.line_delimiter.join(self.batch_data)

    def get_size(self):
        return self.size

    def clear_batch(self):
        self.batch_data.clear()
        self.capacity = 0


class JsonBatch:
    def __init__(self, df_columns: list):
        self.df_columns = df_columns
        self.capacity = 0
        self.batch_data = []

    def get_capacity(self):
        return self.capacity

    def add_data(self, row: list):
        data = {}
        if len(self.df_columns) == len(row):
            for i, column in enumerate(self.df_columns):
                data[column] = row[i]
            batch_data = json.dumps(data, cls=DateEncoder, separators=(",", ":"))
            # self.batch_data.append(data)
            self.batch_data.append(batch_data)
            # self.capacity += len(json.dumps(data, cls=DateEncoder, separators=(",", ":")).encode('utf-8'))
            self.capacity += len(batch_data.encode('utf-8'))

    def get_data(self):
        # return json.dumps(self.batch_data, separators=(",", ":"))
        data = ",".join(self.batch_data)
        return "["+data+"]"

    def get_size(self):
        return self.size

    def clear_batch(self):
        self.batch_data.clear()
        self.capacity = 0


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)
