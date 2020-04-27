import csv
from typing import Union, Dict, Mapping, Sequence

from nxtensor.utils.csv_option_names import CsvOptName


def create_csv_options(separator: str = None, header: int = None, line_terminator: str = None, encoding: str = None,
                       quote_char: str = None, quoting: int = None) -> Dict[CsvOptName, Union[str, int]]:
    result: Dict[CsvOptName, Union[str, int]] = dict()

    if separator:
        result[CsvOptName.SEPARATOR] = separator

    if header:
        result[CsvOptName.HEADER] = header

    if line_terminator:
        result[CsvOptName.LINE_TERMINATOR] = line_terminator

    if encoding:
        result[CsvOptName.ENCODING] = encoding

    if quote_char:
        result[CsvOptName.QUOTE_CHAR] = quote_char

    if quoting:
        result[CsvOptName.QUOTING] = quoting

    return result


DEFAULT_CSV_OPTIONS: Mapping[CsvOptName, any] = {CsvOptName.SEPARATOR: ',',
                                                 CsvOptName.LINE_TERMINATOR: '\\n',
                                                 CsvOptName.HEADER: 0,
                                                 CsvOptName.QUOTE_CHAR: '"',
                                                 CsvOptName.QUOTING: csv.QUOTE_NONNUMERIC,
                                                 CsvOptName.ENCODING: 'utf-8'}


def to_csv(data: Sequence[Mapping[str, any]], file_path: str,
           csv_options: Mapping[CsvOptName, any] = DEFAULT_CSV_OPTIONS) -> None:

    if CsvOptName.ENCODING in csv_options or\
       CsvOptName.LINE_TERMINATOR in csv_options or\
       CsvOptName.SEPARATOR in csv_options or\
       CsvOptName.HEADER in csv_options:
        csv_options = {k: v for k, v in csv_options.items()}

        if CsvOptName.ENCODING in csv_options:
            encoding = csv_options.pop(CsvOptName.ENCODING)
        else:
            encoding = None

        if csv_options[CsvOptName.LINE_TERMINATOR] == DEFAULT_CSV_OPTIONS[CsvOptName.LINE_TERMINATOR]:
            csv_options[CsvOptName.LINE_TERMINATOR] = '\n'

        if CsvOptName.SEPARATOR in csv_options:
            separator = csv_options.pop(CsvOptName.SEPARATOR)
            csv_options['delimiter'] = separator

        if CsvOptName.HEADER in csv_options:
            header = csv_options.pop(CsvOptName.HEADER)
        else:
            header = -1

    if encoding:
        file = open(file_path, 'w', encoding=encoding)
    else:
        file = open(file_path, 'w')

    fieldnames = sorted(data[0].keys())
    csv_writer = csv.DictWriter(file, fieldnames=fieldnames, **csv_options)

    if header >= 0:
        csv_writer.writeheader()

    for mapping in data:
        csv_writer.writerow(mapping)

    file.close()
