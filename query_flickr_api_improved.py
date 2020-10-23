import flickrapi
import re
import json
import urllib
import ssl
import datetime
import time
import os
import requests
from functools import wraps

class FlickrQuerier:
    '''
    IMPORTANT NOTICE:
    the flickr API does not return more than 4'000 results per query even if all returned pages are parsed.
    This is a bug/feature (https://www.flickr.com/groups/51035612836@N01/discuss/72157654309722194/)
    Therefore, queries have to be constructed in a way that less than 4'000 are returned.
    '''
    path_CREDENTIALS = "C:/Users/mhartman/PycharmProjects/MotifDetection/FLICKR_API_KEY.txt"
    path_LOG = "C:/Users/mhartman/PycharmProjects/MotifDetection/LOG_FLICKR_API.txt"
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

    def __init__(self, project_name, area_name, bbox, min_upload_date=None, max_upload_date=None, accuracy=16, toget_images=True, api_creds_file=None, subquery_status=False, allowed_licenses='all'):
        # print("--"*30)
        # print("Initialising Flickr Search with FlickrQuerier Class")
        self.project_name = project_name
        self.project_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), project_name)
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.bbox = bbox
        self.area_name = area_name
        self.min_upload_date = min_upload_date
        self.max_upload_date = max_upload_date
        if self.max_upload_date is None:
            self.max_upload_date = int(time.time())
        self.accuracy = accuracy
        self.toget_images = toget_images
        self.api_creds_file = api_creds_file
        self.subquery_status = subquery_status
        # if allowed_licenses is 'all' then ALL images irrespective of the license shall be returned!
        # could most likely be changed to None to retrieve all licenses but not entirely sure
        self.allowed_licenses = allowed_licenses
        #in case of Connection error time to pause process in seconds
        self.to_sleep = 5
        #check if other api authentication information were provided
        if self.api_creds_file is None:
            self.api_creds_file = FlickrQuerier.path_CREDENTIALS

        self.api_key, self.api_secret = self.load_creds(FlickrQuerier.path_CREDENTIALS)
        # print("--" * 30)
        # print(f"Loading flickr API credentials - done.")
        # print("--" * 30)
        # print(f"Quering flickr API with given bbox: \n{self.bbox}")
        self.unique_ids, self.flickr, self.toomany_pages = self.flickr_search()
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
        print(f"Search - done.")
        print("--" * 30)
        print(f"Fetching metadata for search results and writing to file...")
        self.get_info(self.unique_ids)
        print("--" * 30)
        print(f"Acquiring metadata - done.")
        if self.toget_images:
            print("--" * 30)
            print(f"Downloading images into folder {project_name} to current directory.")
            self.get_images(self.unique_ids, self.flickr)
            print("\n")
            print("--" * 30)
            print(f"Download images - done.")
        print("--" * 30)
        print("--" * 30)
        print("FlickrQuerier Class - done")

    @Decorators.logit
    def load_creds(self, path):
        key_found = False
        secret_found = False
        with open(FlickrQuerier.path_CREDENTIALS, 'r') as f:
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
        flickr = flickrapi.FlickrAPI(self.api_key, self.api_secret, format='json')
        while True:
            try:
                if self.allowed_licenses != 'all':
                    photos = flickr.photos.search(bbox=self.bbox,
                                                  min_upload_date=self.min_upload_date,
                                                  max_upload_date=self.max_upload_date,
                                                  accuracy=self.accuracy,
                                                  license=self.allowed_licenses,
                                                  per_page=250) #is_, accuracy=12, commons=True, page=1, min_taken_date='YYYY-MM-DD HH:MM:SS'
                    break
                else:
                    photos = flickr.photos.search(bbox=self.bbox,
                                                  min_upload_date=self.min_upload_date,
                                                  max_upload_date=self.max_upload_date,
                                                  accuracy=self.accuracy,
                                                  per_page=250)  # is_, accuracy=12, commons=True, page=1, min_taken_date='YYYY-MM-DD HH:MM:SS'
                    break
            except Exception as e:
                print("*" * 30)
                print("*" * 30)
                print("Error occurred: {}".format(e))
                print(f"sleeping {self.to_sleep}s...")
                print("*" * 30)
                print("*" * 30)
                time.sleep(self.to_sleep)

        result = json.loads(photos.decode('utf-8'))
        '''
        Handling for multipage results stored in result_dict
        '''
        pages = result['photos']['pages']
        result_dict = {}
        result_dict['page_1'] = result
        if pages < 15:
            print("Less than 4'000 results for this bounding box. Continuing normally...")
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
                return unique_ids, flickr, toomany_pages

            print(f"Search returned {pages} result pages")
            for page in range(2, pages+1):
                print(f"Querying page {page}...")
                try:
                    if self.allowed_licenses != 'all':
                        result_bytes = flickr.photos.search(bbox=self.bbox,
                                                            min_upload_date=self.min_upload_date,
                                                            max_upload_date=self.max_upload_date,
                                                            accuracy=self.accuracy,
                                                            page=page,
                                                            license=self.allowed_licenses,
                                                            per_page=250)
                        result_dict[f'page_{page}'] = json.loads(result_bytes.decode('utf-8'))
                    else:
                        result_bytes = flickr.photos.search(bbox=self.bbox,
                                                            min_upload_date=self.min_upload_date,
                                                            max_upload_date=self.max_upload_date,
                                                            accuracy=self.accuracy,
                                                            page=page,
                                                            per_page=250)
                        result_dict[f'page_{page}'] = json.loads(result_bytes.decode('utf-8'))

                except Exception as e:
                    print("*" * 30)
                    print("*" * 30)
                    print("Error occurred: {}".format(e))
                    print(f"sleeping {self.to_sleep}s...")
                    print("*" * 30)
                    print("*" * 30)
                    time.sleep(self.to_sleep)
        print("All pages handled.")
        #get ids of returned flickr images
        ids = []
        for dict_ in result_dict:
            for element in result_dict[dict_]['photos']['photo']:
                ids.append(element['id'])
        unique_ids = set(ids)

        print(f"Results found: {len(unique_ids)}")

        return unique_ids, flickr, toomany_pages

    def get_images(self, ids, flickr):
        self.image_path = os.path.join(self.project_path, f'images_{self.project_name}')
        if not os.path.exists(self.image_path):
            os.makedirs(self.image_path)
            print(f"Creating image folder 'images_{self.project_name}' in sub-directory '/{self.project_name}/' - done.")
        else:
            print(f"Image folder 'images_{self.project_name}' exists already in the sub-directory '/{self.project_name}/'.")

        for index, id in enumerate(ids):
            tries = 0
            while True:
                try:
                    tries += 1
                    results = json.loads(flickr.photos.getSizes(photo_id=id).decode('utf-8'))
                    # Medium 640 image size url
                    url_medium = results['sizes']['size'][6]['source']
                    # urllib.request.urlretrieve(url_medium, path) # context=ssl._create_unverified_context()
                    resource = urllib.request.urlopen(url_medium, context=ssl._create_unverified_context())
                    with open(self.image_path + '/' + f"{id}.jpg", 'wb') as image:
                        image.write(resource.read())
                    print(f"\rretrieved {index} of {len(ids)} images", end='')
                    break
                except Exception as e:
                    print(f"\nimage error: {e}")
                    if tries <= 5:
                        print(f"Sleeping for {self.to_sleep}s...")
                        time.sleep(self.to_sleep)
                        continue
                    else:
                        break

    def get_info(self, unique_ids):
        csv_separator = ';'
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

        with open(self.csv_output_path, 'w', encoding='utf-8') as f:
            for index, id in enumerate(unique_ids):
                while True:
                    try:
                        results = json.loads(self.flickr.photos.getInfo(photo_id=id).decode('utf-8'))
                        break
                    except:
                        print(f"Error. Sleeping {self.to_sleep}s")
                        time.sleep(self.to_sleep)
                        #get the top level
                try:
                    results = results['photo']
                except Exception as e:
                    print(f"{e} - No metadata found")
                    continue
                '''
                define which info fields should be fetched.
                ERASE ALL STRINGS OF CSV SEPERATOR! 
                '''
                # extract tags into an string separated by '+'!
                try:
                    tag_string = ''
                    for tag_index, tag in enumerate(results['tags']['tag']):
                        tag_string = tag_string + results['tags']['tag'][tag_index]['_content'].replace(csv_separator, '').replace(tag_connector, '') + tag_connector
                except Exception as e:
                    print(f'\r[-] Tag parsing error: {e} ', end='')
                try:
                    locality = results['location']['locality']['_content'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Locality parsing error: {e} ', end='')
                    locality = ''
                try:
                    county = results['location']['county']['_content'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] County parsing error: {e} ', end='')
                    county = ''
                try:
                    region = results['location']['region']['_content'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Region parsing error: {e} ', end='')
                    region = ''
                try:
                    country = results['location']['country']['_content'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Country parsing error: {e} ', end='')
                    country = ''
                '''
                text clean up
                of title and description
                - remove linebreaks etc.
                '''
                try:
                    description = remove_non_ascii(results['description']['_content'].replace(csv_separator, ''))
                except Exception as e:
                    print(f'\r[-] Description parsing error: {e} ', end='')
                    description = ''
                try:
                    title = remove_non_ascii(results['title']['_content'].replace(csv_separator, ''))
                except Exception as e:
                    print(f'\r[-] Title parsing error: {e} ', end='')
                    title = ''
                try:
                    user_nsid = results['owner']['nsid'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] User_nsid parsing error: {e} ', end='')
                    user_nsid = ''
                try:
                    author_origin = results['owner']['location'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Author_origin parsing error: {e} ', end='')
                    author_origin = ''
                try:
                    date_uploaded = results['dates']['posted'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Date_uploaded parsing error: {e} ', end='')
                    date_uploaded = ''
                try:
                    date_taken = results['dates']['taken'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Date_taken parsing error: {e} ', end='')
                    date_taken = ''
                try:
                    views = results['views'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Views parsing error: {e} ', end='')
                    views = ''
                try:
                    page_url = results['urls']['url'][0]['_content'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Page_URL parsing error: {e} ', end='')
                    page_url = ''
                try:
                    lat = results['location']['latitude'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Latitude parsing error: {e} ', end='')
                    lat = 99999
                try:
                    lng = results['location']['longitude'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Longitude parsing error: {e} ', end='')
                    lng = 99999
                try:
                    accuracy = results['location']['accuracy'].replace(csv_separator, '')
                except Exception as e:
                    print(f'\r[-] Accuracy parsing error: {e} ', end='')
                    accuracy = ''
                data = {
                    'user_nsid': user_nsid,
                    'author_origin': author_origin,
                    'title': title,
                    'description': description,
                    'date_uploaded': date_uploaded,
                    'date_taken': date_taken,
                    'views': views,
                    'page_url': page_url,
                    'user_tags':  tag_string,
                    #location information
                    'lat': lat,
                    'lng': lng,
                    'accuracy': accuracy,
                    'locality': locality,
                    'county': county,
                    'region': region,
                    'country': country
                    }

                if index == 0:
                    header = create_header(data)
                    f.write(f"{header}\n")
                if index % 50 == 0 and index != 0:
                    print(f"\rLine {index} processed", end='')

                line = create_line(id, data)
                f.write(f"{line}\n")

        print(f"\nCreated output file: {self.csv_output_path}")