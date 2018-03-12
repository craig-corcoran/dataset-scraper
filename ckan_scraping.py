import json
import multiprocessing
import os
import time

import requests
from ckanapi import RemoteCKAN

from utilities import get_dataset_name, is_valid_resource, strip_empty


MIN_WAIT = 2
MAX_WAIT = 60


def scrape_ckan_instance(ckan_url="https://open.alberta.ca", formats=['xls', 'xlsx', 'csv'], data_dir='data/ckan'):
    # def scrape_ckan_instance(**kwargs):
    # ckan_url = kwargs['ckan_url'] if kwargs['ckan_url'] else 'https://open.alberta.ca'
    # formats = kwargs['formats'] if kwargs['formats'] else ['xls', 'xlsx', 'csv']
    # data_dir = kwargs['data_dir'] if kwargs['data_dir'] else 'data/ckan'

    def save_metadata(dataset, dataset_folder):  # dataset_name=None, data_dir='data/ckan'):
        dataset_name = dataset_folder.split('/')[-1]  # get_dataset_name(dataset)
        filename = '{0}/{1}_metadata.json'.format(dataset_folder, dataset_name)
        with open(filename, 'w') as json_file:
            metadata = strip_empty(dataset)
            json.dump(metadata, json_file, indent=2)

    def process_resource(resource, dataset_folder, formats=['xls', 'xlsx', 'csv']):
        try:
            # if filetype not in formats, return
            if not resource['url'].split('.')[-1].lower() in formats:
                print('invalid filetype, resource url:', resource['url'])
                return

            resource_fname = resource['url'].split('/')[-1]
            data_filename = '{0}/{1}'.format(dataset_folder, resource_fname)

            # if file already exists and isnt empty, return
            if os.path.isfile(data_filename) and os.path.getsize(data_filename) > 0:
                print('resource already present:', data_filename)
                return

            wait_time = MIN_WAIT
            while True:
                # TODO add timeout to skip large files
                try:
                    # o.w. request resource from url
                    print('requesting', resource['url'])
                    response = requests.get(resource['url'], stream=True)
                    break

                except requests.exceptions.ConnectionError as err:
                    print('connection error requesting from url:', resource['url'])
                    print('error:', err)
                    print('waiting', wait_time, 'seconds ( max is', MAX_WAIT, ')')
                    time.sleep(wait_time)
                    if wait_time < MAX_WAIT:
                        wait_time = min(2*wait_time, MAX_WAIT)  # exponentially increase the wait time until max
                    else:
                        break

            if response and response.status_code == 200:
                # if successful, stream data to file
                print('saving file:', data_filename)
                with open(data_filename, 'wb') as data_file:
                    for chunk in response.iter_content():
                        # if chunk:
                        data_file.write(chunk)

            else:
                print('request failed:', response.status_code)
                print('failed resource:', strip_empty(resource))

        except requests.exceptions.InvalidSchema as err:
            print('invalid shema, not http? url:', resource['url'])
            print('error:', err)

        except Exception as err:
            print('unknown error:', err)
            raise err

    print('scraping ckan instance', ckan_url)
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)

    instance = RemoteCKAN(ckan_url)
    print('retrieving list of instance datasets for', ckan_url)
    datasets = instance.action.current_package_list_with_resources()  # limit, offset)

    print('processing datasets for', ckan_url)
    for dataset in datasets:
        # print(dataset['title'])
        # find resources with valid format etc.
        valid_resources = [resource for resource in dataset['resources'] if is_valid_resource(resource, formats)]
        if valid_resources:
            try:
                dataset_name = get_dataset_name(dataset)
                dataset_folder = '{0}/{1}'.format(data_dir, dataset_name)
                if not os.path.isdir(dataset_folder):
                    os.mkdir(dataset_folder)

                save_metadata(dataset, dataset_folder)

                for resource in valid_resources:
                    process_resource(resource, dataset_folder, formats=formats)

            except Exception as err:
                print('error while scraping ckan resource:', ckan_url)
                print('client dataset obj:', dataset['name'])
                raise err


def parallel_ckan_scrape(formats=['xls', 'xlsx', 'csv', 'json', 'txt'], data_dir='data/ckan'):

    # read in list of ckan instances
    with open('ckan-instances.json') as json_file:
        instance_urls = json.load(json_file)

    scrape_args = [(ckan_url, formats, os.path.join(data_dir, ckan_name))
                   for ckan_name, ckan_url in instance_urls.items()]

    pool = multiprocessing.Pool()
    print('multiprocess mapping scrape func over instance list')
    # pool.starmap(scrape_ckan_instance, scrape_args)
    pool.starmap_async(scrape_ckan_instance, scrape_args)

    pool.close()
    pool.join()


if __name__ == '__main__':
    parallel_ckan_scrape()
