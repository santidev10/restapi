import csv
import io


def build_csv_byte_stream(headers, rows):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    for row in rows:
        clean_row = dict([(k, v) for k, v in row.items() if k in headers])
        writer.writerow(clean_row)
    output.seek(0)
    text_csv = output.getvalue()
    stream = io.BytesIO(text_csv.encode())
    return stream


def get_data_from_csv_response(response):
    content = ""
    for row in response.streaming_content:
        content += row.decode("utf-8")
    csv_content = tuple([data for data in content.split("\n") if data])
    return csv.reader(csv_content)
