import json


class DelimiterParser:
    HEX_STRING = "0123456789ABCDEF"

    @staticmethod
    def parse(sp, d_sp):
        if not sp:
            return d_sp
        if not sp.upper().startswith("\\X"):
            return sp
        hex_str = sp[2:]

        # Check hex str
        if not hex_str:
            raise RuntimeError("Failed to parse delimiter: Hex str is empty")
        if len(hex_str) % 2 != 0:
            raise RuntimeError("Failed to parse delimiter: Hex str length error")
        if any(hex_char not in DelimiterParser.HEX_STRING for hex_char in hex_str.upper()):
            raise RuntimeError("Failed to parse delimiter: Hex str format error")

        # Transform to separator
        bytes_result = bytes.fromhex(hex_str)
        return bytes_result.decode("utf-8")

    @staticmethod
    def hex_str_to_bytes(hex_str):
        upper_hex_str = hex_str.upper()
        length = len(upper_hex_str) // 2
        hex_chars = [upper_hex_str[i:i + 2] for i in range(0, len(upper_hex_str), 2)]
        bytes_result = bytes([int(hex_char, 16) for hex_char in hex_chars])
        return bytes_result


class CopySQLBuilder:
    COPY_SYNC = "copy.async"
    FILE_TYPE = "file.type"

    def __init__(self, copy_into_props, table_identifier, file_name):
        self.data_type = copy_into_props[self.FILE_TYPE]
        self.file_name = file_name
        self.table_identifier = table_identifier
        self.copy_into_props = copy_into_props

    def build_copy_sql(self):
        sb = []
        formatted_file_name = self.file_name.replace("%s", "")
        copy_sql = f"COPY INTO {self.table_identifier} FROM @~('{{{formatted_file_name}}}*') PROPERTIES ("

        sb.append(copy_sql)

        # copy into must be sync
        self.copy_into_props[self.COPY_SYNC] = "false"
        if self.data_type == "json":
            self.copy_into_props["file.strip_outer_array"] = "true"

        copy_into_props_str = {key: str(value).lower() if isinstance(value, bool) else str(value) for key, value in
                               self.copy_into_props.items()}

        properties = json.dumps(copy_into_props_str, separators=(",", "="))[1:-1].replace("\"", "'")
        sb.append(properties)
        sb.append(" )")
        return "".join(sb)
