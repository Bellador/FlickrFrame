import os
import re
import ssl
import time
import json
import urllib
import datetime
import requests
import flickrapi
import concurrent.futures
from functools import wraps

class FlickrQuerier:
    '''
    IMPORTANT NOTICE:
    the flickr API does not return more than 4'000 results per query even if all returned pages are parsed.
    This is a bug/feature (https://www.flickr.com/groups/51035612836@N01/discuss/72157654309722194/)
    Therefore, queries have to be constructed in a way that less than 4'000 are returned.
    '''
    path_LOG = "LOG_FLICKR_API.txt"
    # path_CSV = "C:/Users/mhartman/PycharmProjects/MotiveDetection/wildkirchli_metadata.csv"

    class Decorators:
        # decorator to wrap around functions to log if they are being called
        @classmethod
        def logit(self, func):
            #preserve the passed functions (func) identity - so I doesn't point to the 'wrapper_func'
            @wraps(func)
            def wrapper_func(*args, **kwargs):
                with open(FlickrQuerier.path_LOG, 'at') as log_f:
                    #print("Logging...")
                    log_f.write('-'*20)
                    log_f.write(f'{datetime.datetime.now()} : function {func.__name__} called \n')
                return func(*args, **kwargs)
            return wrapper_func

    def __init__(self, project_name, area_name, bbox=None, text_search=None, tags=None, tag_mode=None, textual_results_to_return=1000, perform_textual_search=False, min_upload_date=None, max_upload_date=None, accuracy=16,
                 toget_images=True, image_size='medium', api_creds_file="C:/Users/mhartman/PycharmProjects/FlickrFrame/FLICKR_API_KEY.txt",
                 subquery_status=False, allowed_licenses='all', rate_limit_sleep=2):

        self.project_name = project_name
        self.project_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), project_name)
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.bbox = bbox
        self.text_search = text_search
        self.tags = tags
        self.tag_mode = tag_mode
        self.textual_results_to_return = textual_results_to_return
        self.perform_textual_search = perform_textual_search
        self.area_name = area_name
        self.min_upload_date = min_upload_date
        self.max_upload_date = max_upload_date
        if self.max_upload_date is None:
            self.max_upload_date = int(time.time())
        self.accuracy = accuracy
        self.toget_images = toget_images
        self.image_size = image_size
        self.api_creds_file = api_creds_file
        self.subquery_status = subquery_status
        # if allowed_licenses is 'all' then ALL images irrespective of the license shall be returned!
        # could most likely be changed to None to retrieve all licenses but not entirely sure
        self.allowed_licenses = allowed_licenses
        # sleep after each api query to not hit the rate limit of 3600 queries per hour source: https://www.flickr.com/services/developer/api/
        self.rate_limit_sleep = rate_limit_sleep
        #in case of Connection error time to pause process in seconds
        self.to_sleep = 60
        self.api_key, self.api_secret = self.load_creds(self.api_creds_file)
        self.result_dict, self.unique_ids, self.flickr, self.toomany_pages = self.flickr_search()
        # check if textual search return limit set by user is reached
        if self.perform_textual_search:
            if self.unique_ids is not None:
                if len(self.unique_ids) > self.textual_results_to_return:
                    print(f'[*] textual search: {self.textual_results_to_return} posts fetched. Finishing search.')
                    print("--" * 30)
                    print(f"[*] Search - done.")
                    print("--" * 30)
                    print(f"[*] Fetching metadata for search results and writing to file...")
                    # needs to be packed into a list for compatibility with high_data_volume_handler that returns a list of result_dicts
                    self.write_info([self.result_dict])
                    print("--" * 30)
                    print(f"[*] Acquiring metadata - done.")
                    if self.toget_images:
                        print("--" * 30)
                        print(f"[*] Downloading images into folder {project_name} to current directory.")
                        self.get_images([self.result_dict], image_size=self.image_size)
                        print("\n")
                        print("--" * 30)
                        print(f"[*] Download images - done.")
                    print("--" * 30)
                    print("--" * 30)
                    print("[*] FlickrQuerier Class - done")
        '''
        Check if too many pages (15 or more)
        were returned by the flickr search for the given bounding box
        toomany_pages is a tuple with amount of pages and boolean of True if pages >= 15
        '''
        if self.toomany_pages[1]:
            return None
        if self.subquery_status:
            # print("Fetching unique ids of subquery")
            return None
        print("--" * 30)
        print(f"[*] Search - done.")
        print("--" * 30)
        print(f"[*] Fetching metadata for search results and writing to file...")
        # needs to be packed into a list for compatibility with high_data_volume_handler that returns a list of result_dicts
        self.write_info([self.result_dict])
        print("--" * 30)
        print(f"[*] Acquiring metadata - done.")
        if self.toget_images:
            print("--" * 30)
            print(f"[*] Downloading images into folder {project_name} to current directory.")
            self.get_images([self.result_dict], image_size=self.image_size)
            print("\n")
            print("--" * 30)
            print(f"[*] Download images - done.")
        print("--" * 30)
        print("--" * 30)
        print("[*] FlickrQuerier Class - done")

    @Decorators.logit
    def load_creds(self, path):
        key_found = False
        secret_found = False
        with open(self.api_creds_file, 'r') as f: #FlickrQuerier.path_CREDENTIALS
            for line in f:
                if key_found:
                    api_key = line.strip().encode('utf-8')
                    key_found = False

                if secret_found:
                    api_secret = line.strip().encode('utf-8')
                    secret_found = False

                if re.match(r'<KEY>', line):
                    key_found = True
                    continue
                elif re.match(r'<SECRET>', line):
                    secret_found = True
        return api_key, api_secret

    def flickr_search(self):
        # extra fields to retrieve with photos.search endpoint. Can replace the much more request intensive photos.getInfo!
        # must be a comma seperated list - but still a string; not a python list!
        extras = "description, license, date_upload, date_taken, owner_name, icon_server, original_format, last_update, " \
                 "geo, tags, machine_tags, o_dims, views, media, path_alias, url_sq, url_t, url_s, url_q, url_m, url_n, " \
                 "url_z, url_c, url_l, url_o"
        flickr = flickrapi.FlickrAPI(self.api_key, self.api_secret, format='json')
        # check if bbox or text based search shall be performed
        # if self.bbox is not None and self.text_search is None:
        while True:
            try:
                if self.allowed_licenses != 'all':
                    photos = flickr.photos.search(bbox=self.bbox,
                                                  text=self.text_search,
                                                  tags=self.tags,
                                                  tag_mode=self.tag_mode,
                                                  min_upload_date=self.min_upload_date,
                                                  max_upload_date=self.max_upload_date,
                                                  accuracy=self.accuracy,
                                                  license=self.allowed_licenses,
                                                  per_page=250,
                                                  extras=extras) #is_, accuracy=12, commons=True, page=1, min_taken_date='YYYY-MM-DD HH:MM:SS'
                    break
                else:
                    photos = flickr.photos.search(bbox=self.bbox,
                                                  text=self.text_search,
                                                  tags=self.tags,
                                                  tag_mode=self.tag_mode,
                                                  min_upload_date=self.min_upload_date,
                                                  max_upload_date=self.max_upload_date,
                                                  accuracy=self.accuracy,
                                                  per_page=250,
                                                  extras=extras)# is_, accuracy=12, commons=True, page=1, min_taken_date='YYYY-MM-DD HH:MM:SS'
                    break

            except Exception as e:
                print("*" * 30)
                print("*" * 30)
                print(f"[-] Search error: {e}")
                print(f"[*] Sleeping {self.to_sleep}s...")
                print("*" * 30)
                print("*" * 30)
                time.sleep(self.to_sleep)

        time.sleep(self.rate_limit_sleep)  # according to the rate limit of 3600 queries per hour source: https://www.flickr.com/services/developer/api/

        result = json.loads(photos.decode('utf-8'))
        '''
        Handling for multipage results stored in result_dict
        '''
        pages = result['photos']['pages']
        result_dict = {}
        result_dict['page_1'] = result
        if pages < 15:
            print("[*] Less than 4'000 results for this bounding box. Continuing normally...")
            toomany_pages = (pages, False)
        if pages != 1 and pages != 0:
            '''
            Checking if (4'000 % 250 = 16; for caution purposes 15) pages are returned, 
            since that would exceed the maximum of 4'000 returnable results for a single query.
            Adapt query according to the amount of returned pages to allow to acquire all possible data            
            '''
            if pages >= 15:
                print(f"CAUTION: {pages} pages returned. \nAutomatically adapting query to retrieve all posts...")
                unique_ids = None
                toomany_pages = (pages, True)
                return result_dict, unique_ids, flickr, toomany_pages

            print(f"Search returned {pages} result pages")
            for page in range(2, pages+1):
                print(f"Querying page {page}...")
                try:
                    if self.allowed_licenses != 'all':
                        result_bytes = flickr.photos.search(bbox=self.bbox,
                                                            text=self.text_search,
                                                            tags=self.tags,
                                                            tag_mode=self.tag_mode,
                                                            min_upload_date=self.min_upload_date,
                                                            max_upload_date=self.max_upload_date,
                                                            accuracy=self.accuracy,
                                                            page=page,
                                                            license=self.allowed_licenses,
                                                            per_page=250,
                                                            extras=extras)
                        result_dict[f'page_{page}'] = json.loads(result_bytes.decode('utf-8'))
                    else:
                        result_bytes = flickr.photos.search(bbox=self.bbox,
                                                            text=self.text_search,
                                                            tags=self.tags,
                                                            tag_mode=self.tag_mode,
                                                            min_upload_date=self.min_upload_date,
                                                            max_upload_date=self.max_upload_date,
                                                            accuracy=self.accuracy,
                                                            page=page,
                                                            per_page=250,
                                                            extras=extras)
                        result_dict[f'page_{page}'] = json.loads(result_bytes.decode('utf-8'))
                    time.sleep(self.rate_limit_sleep)  # according to the rate limit of 3600 queries per hour source: https://www.flickr.com/services/developer/api/

                except Exception as e:
                    print("*" * 30)
                    print("*" * 30)
                    print(f"[-] Search error: {e}")
                    print(f"[*] Sleeping {self.to_sleep}s...")
                    print("*" * 30)
                    print("*" * 30)
                    time.sleep(self.to_sleep)
        print("[*] All pages handled.")
        # get ids of returned flickr images
        ids = []
        for dict_ in result_dict:
            try:
                for element in result_dict[dict_]['photos']['photo']:
                    ids.append(element['id'])
            except Exception as e:
                print(f'[-] Error in result_dict: {e}')
        unique_ids = set(ids)

        print(f"[*] Results found: {len(unique_ids)}")

        return result_dict, unique_ids, flickr, toomany_pages

    def get_images(self, results_list, image_size='medium', WORKERS=10):
        def download_urls(data_chunk):
            '''
            download images in parallel
            :return:
            '''
            worker_id = data_chunk[0]
            image_path = data_chunk[1]
            url_chunk = data_chunk[2]
            url_len = len(url_chunk)

            images_dowloaded = 0
            for index, img_tuple in enumerate(url_chunk, 0):
                tries = 0
                img_id = img_tuple[0]
                img_url = img_tuple[1]
                while True:
                    try:
                        tries += 1
                        resource = urllib.request.urlopen(img_url, context=ssl._create_unverified_context())
                        with open(image_path + '/' + f"{img_id}.jpg", 'wb') as image:
                            image.write(resource.read())
                        images_dowloaded += 1
                        print(f"\r[+] WORKER {worker_id}: retrieved {images_dowloaded} of {url_len} images", end='')
                        break
                    except Exception as e:
                        print(f"\n[-] Image error: {e}")
                        if tries <= 5:
                            print(f"[*] Sleeping {5}s...")
                            time.sleep(5)
                            continue
                        else:
                            break

        self.image_path = os.path.join(self.project_path, f'images_{self.project_name}')
        if not os.path.exists(self.image_path):
            os.makedirs(self.image_path)
            print(f"[*] Creating image folder 'images_{self.project_name}' in sub-directory '/{self.project_name}/' - done.")
        else:
            print(f"[*] Image folder 'images_{self.project_name}' exists already in the sub-directory '/{self.project_name}/'.")

        # dict that matches image_size to dictionary key as it is in the Flickr API response
        image_size_dict = {'small': 'url_s',
                           'medium': 'url_m',
                           'large': 'url_l',
                           'original': 'url_o'}
        image_size_key = image_size_dict[image_size]
        # add a empty data chunk for each worker
        data_chunk_list = [[] for worker in list(range(WORKERS))]

        processed_ids_set = set()
        worker_index = 0
        for results in results_list:
            for index_1, page in enumerate(results):
                for post in results[page]['photos']['photo']:
                    img_id = post['id']
                    if img_id in processed_ids_set:
                        continue
                    else:
                        processed_ids_set.add(img_id)
                        try:
                            img_url = post[image_size_key]
                        except:
                            list_of_alternative_image_urls = ['url_l', 'url_o', 'url_s', 'url_q', 'url_sq', 'url_t']
                            for url_key in list_of_alternative_image_urls:
                                try:
                                    img_url = post[url_key]
                                    break
                                except:
                                    continue
                                print(f'[!] Error while fetching size specific img url: {e}')

                        data_tuple = (img_id, img_url)
                        if worker_index >= WORKERS:
                            worker_index = 0
                        data_chunk_list[worker_index].append(data_tuple)
                        worker_index += 1
        # add worker_id and image_path to each data chunk
        data_packages = []
        for worker_id, data_chunk in enumerate(data_chunk_list, 1):
            data_package = [worker_id, self.image_path, data_chunk]
            data_packages.append(data_package)
        # spawn workers with data_package
        start = time.time()
        # download_urls(data_packages[0])
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            # map waits in itself for all workers to finish
            executor.map(download_urls, data_packages)
        end = time.time()
        print(f'\n[*] downloaded images in: {round((end - start) / 60, 2)} min')

    def write_info(self, results_list):
        csv_separator = ';'  #';' #~&~#
        tag_connector = '+'

        def remove_non_ascii(s):
            return "".join(i for i in s if ord(i) < 126 and ord(i) > 31)

        def create_header(data_dict):
            header_string = f'photo_id{csv_separator}'
            for tracker, element in enumerate(data_dict.keys(), 1):
                if tracker < len(data_dict.keys()):
                    header_string = header_string + str(element) + csv_separator
                elif tracker == len(data_dict.keys()):
                    header_string = header_string + str(element)
            return header_string

        def create_line(id, data_dict):
            line = f'{id}{csv_separator}'
            tracker = 1
            for key, value in data_dict.items():
                if tracker < len(data_dict.keys()):
                    line = line + str(value) + csv_separator
                elif tracker == len(data_dict.keys()):
                    line = line + str(value)
                tracker += 1
            return line

        self.csv_output_path = self.dir_path + '/{}/metadata_{}_{:%Y_%m_%d}.csv'.format(self.project_name, self.area_name, datetime.datetime.now())

        processed_ids_set = set()
        with open(self.csv_output_path, 'w', encoding='utf-8') as f:
                index = 0
                for results in results_list:
                    for page in results:
                        for post in results[page]['photos']['photo']:
                            index += 1
                            post_id = post['id']
                            # check if this post_id was already added to the output CSV
                            if post_id in processed_ids_set:
                                continue
                            else:
                                processed_ids_set.add(post_id)
                                '''
                                define which info fields should be fetched.
                                ERASE ALL STRINGS OF CSV SEPERATOR! 
                                '''
                                # extract tags into an string separated by '+'!
                                try:
                                    tag_string = tag_connector.join(post['tags'].replace(csv_separator, '').replace(tag_connector, '').split(' '))
                                except Exception as e:
                                    # print(f'\r[-] Tag parsing error: {e} ', end='')
                                    tag_string = ''
                                try:
                                    machine_tag_string = tag_connector.join(post['machine_tags'].replace(csv_separator, '').replace(tag_connector, '').split(' '))
                                except Exception as e:
                                    # print(f'\r[-] Tag parsing error: {e} ', end='')
                                    machine_tag_string = ''
                                '''
                                text clean up
                                of title and description
                                - remove linebreaks etc.
                                '''
                                try:
                                    description = remove_non_ascii(post['description']['_content'].replace(csv_separator, ''))
                                except Exception as e:
                                    # print(f'\r[-] Description parsing error: {e} ', end='')
                                    description = ''
                                try:
                                    title = remove_non_ascii(post['title'].replace(csv_separator, ''))
                                except Exception as e:
                                    # print(f'\r[-] Title parsing error: {e} ', end='')
                                    title = ''
                                try:
                                    user_nsid = post['owner']
                                except Exception as e:
                                    # print(f'\r[-] User_nsid parsing error: {e} ', end='')
                                    user_nsid = ''
                                try:
                                    date_uploaded = post['dateupload'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Date_uploaded parsing error: {e} ', end='')
                                    date_uploaded = ''
                                try:
                                    date_taken = post['datetaken'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Date_taken parsing error: {e} ', end='')
                                    date_taken = ''
                                try:
                                    views = post['views'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Views parsing error: {e} ', end='')
                                    views = ''
                                try:
                                    license = post['license'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Views parsing error: {e} ', end='')
                                    license = ''
                                try:
                                    img_url_s = post['url_s'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Page_URL parsing error: {e} ', end='')
                                    img_url_s = ''
                                try:
                                    img_url_m = post['url_m'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Page_URL parsing error: {e} ', end='')
                                    img_url_m = ''
                                try:
                                    img_url_l = post['url_l'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Page_URL parsing error: {e} ', end='')
                                    img_url_l = ''
                                try:
                                    img_url_o = post['url_o'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Page_URL parsing error: {e} ', end='')
                                    img_url_o = ''
                                try:
                                    lat = post['latitude'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Latitude parsing error: {e} ', end='')
                                    lat = 99999
                                try:
                                    lng = post['longitude'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Longitude parsing error: {e} ', end='')
                                    lng = 99999
                                try:
                                    woeid = post['woeid'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Longitude parsing error: {e} ', end='')
                                    woeid = 99999
                                try:
                                    place_id = post['place_id'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Longitude parsing error: {e} ', end='')
                                    place_id = 99999
                                try:
                                    accuracy = post['accuracy'].replace(csv_separator, '')
                                except Exception as e:
                                    # print(f'\r[-] Accuracy parsing error: {e} ', end='')
                                    accuracy = ''
                                data = {
                                    'user_nsid': user_nsid,
                                    'title': title,
                                    'description': description,
                                    'date_uploaded': date_uploaded,
                                    'date_taken': date_taken,
                                    'views': views,
                                    'user_tags':  tag_string,
                                    'machine_tags': machine_tag_string,
                                    'license': license,
                                    #location information
                                    'lat': lat,
                                    'lng': lng,
                                    'woeid': woeid,
                                    'place_id': place_id,
                                    'accuracy': accuracy,
                                    # image urls in different sizes
                                    'image_url_small': img_url_s,
                                    'image_url_medium': img_url_m,
                                    'image_url_large': img_url_l,
                                    'image_url_original': img_url_o
                                    }

                                if index == 1:
                                    header = create_header(data)
                                    f.write(f"{header}\n")
                                if index % 50 == 0 and index != 0:
                                    print(f"\rLine {index} processed", end='')

                                line = create_line(post_id, data)
                                f.write(f"{line}\n")

        print(f"\nCreated output file: {self.csv_output_path}")