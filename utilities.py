import json
import csv
import xlrd

from ckanapi import RemoteCKAN


def test_ckan_instances(instances_file='data/ckan/ckan-instances.json'):
    with open(instances_file) as json_file:
        instances = json.load(json_file)

    problems = []
    for ckan_url in instances:
        instance = RemoteCKAN(ckan_url)
        try:
            datasets = instance.action.current_package_list_with_resources()
            print('success:', ckan_url)
        except Exception as err:
            print('error:', err)
            print('problem with instance, url:', ckan_url)
            problems.append(ckan_url)

    print('problem urls:', problems)


def excel_to_csv(excel_filename, csv_filename):
    with xlrd.open_workbook(excel_filename) as workbook:
        assert len(workbook.sheets) == 1
        sh = workbook.sheet_by_index(0)
        with open(csv_filename, 'w') as csv_file:
            writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
            for row in range(sh.nrows):
                writer.writerow(sh.row_values(row))


def strip_empty(to_strip):
    if isinstance(to_strip, str) or isinstance(to_strip, bool) or isinstance(to_strip, int):
        return to_strip

    elif isinstance(to_strip, list):
        new_list = [strip_empty(val) for val in to_strip if val]
        if to_strip == new_list:
            return new_list
        else:
            return strip_empty(new_list)

    elif isinstance(to_strip, dict):
        new_dict = {key: strip_empty(val) for (key, val) in to_strip.items() if val}
        if to_strip == new_dict:
            return new_dict
        else:
            return strip_empty(new_dict)

    else:
        raise Exception('invalid input type: {0}, should be string or dict'.format(type(to_strip)))


def get_dataset_name(dataset, safe_chars=['-', '.', '_']):
    name = dataset['title'].lower().replace(' ', '-')
    name = ''.join(c for c in name if c.isalnum() or c in safe_chars)
    return name.replace('----', '-').replace('---', '-').replace('--', '-')


def is_valid_resource(resource, formats=['xls', 'xlsx', 'csv']):
    # TODO size limitation?
    return resource['format'].lower() in formats  # and (resource['size'] > 0)
