from datadotworld.client import api as ddw
import requests
import json
import pandas as pd
import numpy as np
import boto3
import os
import glob
import scipy.sparse as sp
# from embedding import Embedding
from utils import get_timestamp, write_json, read_json
from dotenv import load_dotenv
import multiprocessing
import time

load_dotenv(dotenv_path='ddw.env')
TOKEN = os.getenv("TOKEN")
SECRET_KEY = os.getenv("SECRET_KEY")
ACCESS_KEY = os.getenv("ACCESS_KEY")


def scrape_ddw(user='craig-corcoran', project='dataset-labeling'):

    req_params = {'headers': {'Authorization': 'Bearer {0}'.format(TOKEN)}}
    response = requests.get('https://api.data.world/v0/projects/{0}/{1}'.format(user, project), **req_params)
    content = json.loads(response.content)

    datasets = content['linkedDatasets']

    base_url = 'https://api.data.world/v0'

    tags = {}
    for ds in datasets:
        key = '{0}/{1}'.format(ds['owner'], ds['id'])
        file_key = key.replace('/', '_')
        print('processing dataset:', file_key)

        dat_url = '{0}/datasets/{1}'.format(base_url, key)
        response = requests.get(dat_url, **req_params)
        ds_content = json.loads(response.content)
        tags[key] = ds_content['tags']

        with open('data/ddw/{0}_tags.json'.format(file_key), 'w') as json_file:
            json.dump(ds_content['tags'], json_file, indent=2)

        # get table names
        table_query = 'SELECT * FROM Tables'
        sql_url = '{0}/sql/{1}'.format(base_url, key)
        tables = requests.get(sql_url, params={'query': table_query}, **req_params)
        tables = json.loads(tables.content)

        for table in tables:
            if table:
                table_name = table['tableId']
                if table_name:
                    # print('reading from table:', table_name)
                    data_query = 'SELECT * FROM {0}'.format(table_name)
                    # , 'includeTableSchema': False (in params)
                    data = requests.get(sql_url, params={'query': data_query}, **req_params)
                    if data.status_code == 200:
                        try:
                            data = json.loads(data.content)
                            df = pd.DataFrame(data)
                            data_filepath = 'data/ddw/{0}_{1}.csv'.format(file_key, table_name)
                            # print('saving to file:', data_filepath)
                            df.to_csv(data_filepath, index=False)

                        except Exception as e:
                            print('error:', e)
                    else:
                        print('request failed:', data.__dict__)
                else:
                    print('missing table id:', table)
            else:
                print('missing table in:', tables)


def metadata_present(metadata_fname):
    return os.path.isfile(metadata_fname) and (os.path.getsize(metadata_fname) > 0)


def process_bucket_object(obj_key, base_dir, base_url, req_params, bucket_name, formats):

    # print('processing object: ', obj)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # object keys are of the form: derived/<owner>/<dataset-name>/<file-name>
    # remove "derived" prefix, replace "/" in path with "." for filename
    key_list = obj_key.split('/')
    owner = key_list[1]
    data_key = key_list[2]
    fname = '.'.join(key_list[3:])
    data_id = '{0}.{1}'.format(owner, data_key)

    # print('processing object: ', data_id)

    dir_path = '{0}/{1}'.format(base_dir, data_id)
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)

    # TODO handle deeper nested directories (getting "does not exist" from ddw api)
    metadata_fname = '{0}/{1}_metadata.json'.format(dir_path, data_id)
    if not metadata_present(metadata_fname):
        try:
            # print('getting metadata for dataset: {0}'.format(data_id))
            response = requests.get('{0}/datasets/{1}/{2}'.format(base_url, owner, data_key), **req_params)
            if isinstance(response.content, str):
                content = json.loads(response.content)
            else:
                content = json.load(response)

            write_json(content, metadata_fname)
            if content.get('tags'):
                # print('found tags from', metadata_fname, content['tags'])
                tags_fname = '{0}/{1}_tags.json'.format(dir_path, data_id)
                write_json(content['tags'], tags_fname)

        except Exception as e:
            print('error with metadata in:', metadata_fname)
            print('error:', e)
            raise e

    # if file not already present,
    data_fname = '{0}/{1}'.format(dir_path, fname)
    file_format = os.path.splitext(fname)[1].replace('.', '')
    if os.path.isfile(data_fname) and os.path.getsize(data_fname) > 0:
        print('file already present:', data_fname)

    elif file_format in formats:
        try:
            # print('saving file:', data_fname)
            bucket.download_file(obj_key, data_fname)
        except Exception as e:
            print('error with file', obj_key)
            print('error:', e)
    # else:
    #     print('invalid format:', data_fname)


def read_s3_parallel(base_dir='data/ddw-s3', bucket_name='dataworld-newknowledge-us-east-1', formats=['xls', 'xlsx', 'csv', 'json', 'txt'], batch_size=64):

    req_params = {'headers': {'Authorization': 'Bearer {0}'.format(TOKEN)}}
    base_url = 'https://api.data.world/v0'

    s3 = boto3.resource('s3',
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY,
                        )

    bucket = s3.Bucket(bucket_name)
    obj_gen = bucket.objects.filter(Prefix='derived')

    if not os.path.isdir(base_dir):
        os.makedirs(base_dir)

    for i, page in enumerate(obj_gen.pages()):
        print('page', i)
        with multiprocessing.Pool() as pool:
            scrape_args = [(obj.key, base_dir, base_url, req_params, bucket_name, formats) for obj in page]
            print('multiprocess mapping scrape func over batch of', len(scrape_args), 'objects from bucket')
            # pool.starmap_async(process_bucket_object, scrape_args)
            pool.starmap(process_bucket_object, scrape_args)
            time.sleep(2)

        pool.join()

    # pool.close()

    # pool.starmap(process_bucket_object, scrape_args)

    # scrape_args = ((obj, base_dir, base_url, req_params, bucket, formats)
    #                for obj in bucket.objects.filter(Prefix='derived'))

    # for obj in bucket.objects.filter(Prefix='derived'):
    #     process_bucket_object(obj, base_dir, base_url, req_params, bucket, formats)

    # # object keys are of the form: derived/<owner>/<dataset-name>/<file-name>
    # # remove "derived" prefix, replace "/" in path with "." for filename
    # key_list = obj.key.split('/')
    # owner = key_list[1]
    # data_key = key_list[2]
    # fname = '.'.join(key_list[3:])
    # data_id = '{0}.{1}'.format(owner, data_key)

    # dir_path = '{0}/{1}'.format(base_dir, data_id)
    # if not os.path.isdir(dir_path):
    #     os.makedirs(dir_path)

    # # TODO handle deeper nested directories (getting "does not exist" from ddw api)
    # metadata_fname = '{0}/{1}_metadata.json'.format(dir_path, data_id)
    # if not metadata_present(metadata_fname):
    #     try:
    #         print('getting metadata for dataset: {0}'.format(data_id))
    #         response = requests.get('{0}/datasets/{1}/{2}'.format(base_url, owner, data_key), **req_params)
    #         content = json.loads(response.content)
    #         write_json(content, metadata_fname)
    #         if not content.get('tags'):
    #             print('missing or empty tags list:', metadata_fname)
    #             # print('content:', content)
    #         else:
    #             print('found tags from', metadata_fname, content['tags'])
    #             tags_fname = '{0}/{1}_tags.json'.format(dir_path, data_id)
    #             write_json(content['tags'], tags_fname)

    #     except Exception as e:
    #         print('error with metadata in:', metadata_fname)
    #         print('response:', content)
    #         print('error:', e)
    #         raise e

    # save_fname = '{0}/{1}'.format(dir_path, fname)
    # if not os.path.isfile(save_fname):
    #     try:
    #         print('saving file:', save_fname)
    #         bucket.download_file(obj.key, save_fname)
    #     except Exception as e:
    #         print('error with file', obj.key)
    #         print('error:', e)


def get_data_id(base_dir, folder):
    return folder.replace(base_dir, '').replace('/', '')


def move_labeled(base_dir='data/ddw-s3'):

    labeled_dir = '{0}/labeled'.format(base_dir)
    if not os.path.isdir(labeled_dir):
        os.mkdir(labeled_dir)

    directories = glob.glob('{0}/*'.format(base_dir))
    for folder in directories:
        data_id = get_data_id(base_dir, folder)
        tags_fname = get_tags_filename(folder, data_id)

        if metadata_present(tags_fname):
            with open(tags_fname) as json_file:
                tags = json.load(json_file)
                if tags:
                    print('moving folder', folder, 'to', '{0}/labeled/{1}'.format(base_dir, data_id))
                    os.rename(folder, '{0}/labeled/{1}'.format(base_dir, data_id))
                else:
                    print('empty tag file, delete?')
        else:
            print('no tag file for:', folder)


def get_tags_filename(folder, data_id):
    return '{0}/{1}_tags.json'.format(folder, data_id)


def get_all_tags(base_dir='data/ddw-s3/labeled', n_tags=1000):

    directories = glob.glob('{0}/*'.format(base_dir))
    all_tags = set()
    tag_freq = {}
    tags_dict = {}

    for folder in directories:
        # print('processing tags in:', folder)
        # data_id = folder.replace(base_dir, '').replace('/', '')
        data_id = get_data_id(base_dir, folder)
        # tags_fname = '{0}/{1}_tags.json'.format(folder, data_id)
        tags_fname = get_tags_filename(folder, data_id)
        if metadata_present(tags_fname):
            with open(tags_fname) as json_file:
                tags = json.load(json_file)
                if tags:
                    tags_dict[data_id] = tags
                    all_tags.update(tags)
                    for tag in tags:
                        tag_freq[tag] = 1 + tag_freq.get(tag, 0)
                else:
                    print('empty tags list in:', folder)
        else:
            print('missing/empty tags in:', folder)

    print('all tags:', all_tags)
    freq_vals = list(tag_freq.values())
    sort_ind = np.argsort(freq_vals)[::-1]
    sorted_keys = np.array(list(tag_freq.keys()))[sort_ind]
    sorted_values = np.array(freq_vals)[sort_ind]
    print('top keys:')
    for key, val in zip(sorted_keys[:n_tags], sorted_values[:n_tags]):
        print(key, val)

    print('total number of tags:', len(all_tags))

    dict_path = '{0}/tags_dict.json'.format(base_dir)
    with open(dict_path, 'w') as json_file:
        print('writing all tags to file')
        json.dump(tags_dict, json_file, indent=2)


def build_target_matrix(base_dir='data/ddw-s3'):

    dict_path = '{0}/tags_dict.json'.format(base_dir)
    with open(dict_path) as json_file:
        print('loading tags from file')
        tags_dict = json.load(json_file)

    unique_tags = list(set([tag for tag_list in tags_dict.values() for tag in tag_list]))
    print('unique tags:', unique_tags)
    data_ids = list(tags_dict.keys())
    n_tags = len(unique_tags)
    n_datasets = len(data_ids)
    print('number of unique tags:', n_tags)

    tag_to_index = {tag: ind for (ind, tag) in enumerate(unique_tags)}

    print('computing nonzero indices')
    row_inds = np.concatenate([[ind]*len(data_tags) for (ind, data_tags) in enumerate(tags_dict.values())])
    col_inds = np.array([tag_to_index[tag] for data_tags in tags_dict.values() for tag in data_tags])

    assert len(row_inds) == len(col_inds)
    n_nonzero = len(col_inds)
    data = np.ones(n_nonzero, dtype=int)

    print('building csr matrix')
    target_matrix = sp.csr_matrix((data, (row_inds, col_inds)), shape=(n_datasets, n_tags))

    print('saving target data to file')
    timestamp = get_timestamp()
    matrix_filename = '{0}/target-matrix_{1}.npz'.format(base_dir, timestamp)
    sp.save_npz(matrix_filename, target_matrix)
    np.save('{0}/tags_{1}.npy'.format(base_dir, timestamp), unique_tags)
    np.save('{0}/dataset-ids_{1}.npy'.format(base_dir, timestamp), data_ids)

    return {
        'target_matrix': target_matrix,
        'tags': unique_tags,
        'dataset_ids': data_ids,
    }

    # compare tags to vocab of word embedding
    # vectorize datasets
    # perform multi-label classification

    # test target matrix creation?
    # refine tags list?


if __name__ == '__main__':
    # main()
    read_s3_parallel()
    # get_all_tags()
    # build_target_matrix()
    # move_labeled()
