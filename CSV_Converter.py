import xml.etree.ElementTree as Xet
import pandas as pd
import sys
import os

namespaces = {"dc": "http://purl.org/dc/elements/1.1/"}


def get_string_format(root, type):

    type = root.findall("dc:" + type, namespaces)
    for t in type:
        return t.text.rstrip()


columns = [
    "source_identifier",
    "title",
    "creator",
    "description",
    "publisher",
    "date_created",
    "resource_type",
    "language",
    "rights_statement",
]
rows = []

filename = sys.argv[-1]
xmlparse = Xet.parse(filename)

root = xmlparse.getroot()

rows.append(
    {
        "source_identifier": "Work One",
        "title": get_string_format(root, "title"),
        "creator": get_string_format(root, "creator"),
        "description": get_string_format(root, "description"),
        "publisher": get_string_format(root, "publisher"),
        "date_created": get_string_format(root, "date"),
        "resource_type": get_string_format(root, "type"),
        "language": get_string_format(root, "language"),
        "rights_statement": get_string_format(root, "rights"),
    }
)

df = pd.DataFrame(rows, columns=columns)
df.to_csv("metadata.csv", encoding="utf-8", index=False)
