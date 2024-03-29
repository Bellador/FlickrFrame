import os
import sys
import json
import datetime
from time import time
from query_flickr_api_improved import FlickrQuerier

already_processed = [] #'DE-HW', 'CH-FM', 'CH-SB', 'GR-KA', 'SP-MO', 'SP-SC', 'RO-SA', 'PT-MN', 'FR-CL', 'UK-WB', 'SE-LI', 'SP-LT'

class FlickrFrame:
    '''
    Overview:
    This script/class is used to easily and customizable query the official FlickrAPI for georeferenced posts
    by supplying the boundaries of an area of interest either through a single bounding box or through a GeoJson
    file that encompasses multiple different bounding boxes. It also allows to simultanously download the corresponding
    flickr images to save and store them locally.

    During the process it is checked if a query exceeds the maximum of 4'000 results returned.
    If that is the case, the same bounding box is queried iteratively with smaller timespans to capture all possible
    georeferenced flickr posts from a given region.

    The output is presented in a CSV file with semicolon seperation. All the data is UTF-8 encoded and processed to
    allow for easy further processing.
    The name of the output file is given , geojson the name field is used in the output file name

    Workspace will be established in the same directory as this file.

    API AUTHENTICATION:
    During the FlickrQuerier class invokation a (txt) file has to be provided which contains <KEY> and <SECRET> sections
    where the users personal authenticatoin details are contained.
    '''
    def __init__(self, project_name, api_credentials_path, min_upload_date=None, max_upload_date=None, bbox=None, text_search=None, tags=None,
                            tag_mode=None, has_geo=True, textual_results_to_return=1000, geojson_file=None, accuracy=16, toget_images=True, image_size='medium', allowed_licenses=None):
        self.project_name = project_name
        self.api_credentials_path = api_credentials_path
        self.min_upload_date = min_upload_date
        self.max_upload_date = max_upload_date
        self.bbox = bbox
        self.text_search = text_search
        self.tags = tags
        self.tag_mode = tag_mode
        self.has_geo = has_geo
        self.textual_results_to_return = textual_results_to_return
        self.perform_textual_search = False
        if self.tag_mode is not None or self.text_search is not None:
            self.perform_textual_search = True
        self.geojson_file = geojson_file
        self.accuracy = accuracy
        self.toget_images = toget_images
        self.image_size = image_size
        self.allowed_licenses = allowed_licenses
        '''
        Check if the user supplied:
            1. single bbox or 
            2. bbox countained in a geojson file
            3. Alert if none was supplied
            4. Alert if both were supplied - which to use?
        '''
        if self.bbox is None and self.geojson_file is None and self.text_search is None and self.tags is None:
            print("No bbox or text search information supplied (neither tags nor free text). \nAborting...")
            sys.exit(1)
        elif self.bbox is not None and self.geojson_file is not None:
            print("Single bbox and GeoJson supplied. Supply only one.\nAborting...")
            sys.exit(1)
        elif self.bbox is not None and self.geojson_file is None and (self.text_search is not None or self.tags is not None):
            print("Both bbox and textual search is supplied. All except the one has to be None. \nAborting...")
            sys.exit(1)
        elif self.text_search is not None and self.tags is not None:
            print("Both textual search are supplied. Either tag or free text has to be chosen. Not both. \nAborting...")
            sys.exit(1)
        self.body()

    def high_data_volume_handler(self, pages, textual_results_to_return, perform_textual_search):
        '''
        Query one bounding box multiple times to retrieve all possible results.
        This is done by adapting the queried timespan according to the amount of result pages.
        At the end the CSV files of all subqueries are merged.

        :param bbox:
        :param pages:
        :return:
        '''
        tries = 0
        while True:
            tries += 1
            upper_limit_timespan = None
            lower_limit_timespan = None
            #define current timespan (if none for max defined as the unixtimestamp of right now and min as the start of flickr)
            if self.max_upload_date is None:
                upper_limit_timespan = time()
            else:
                upper_limit_timespan = self.max_upload_date

            if self.min_upload_date is None:
                lower_limit_timespan = 950659200 #Equals date: 16.02.2000
            else:
                lower_limit_timespan = self.min_upload_date

            timespan = upper_limit_timespan - lower_limit_timespan
            #split timespan according to pages
            timespan_subquery = int(round(timespan / (pages * tries), 0))
            '''
            aggregate the result dicts of the subquery to form one final result dict that is written to the output file
            as a complete set of the bbox
            '''
            all_unique_ids = set()
            final_result_dict_list = []
            # count the total amount of results returned to apply the textual query return limit set by the user
            total_returned_results = 0
            for counter in range((pages * tries)):
                new_lower_limit = lower_limit_timespan
                new_upper_limit = lower_limit_timespan + timespan_subquery
                print("--" * 30)
                print(f"[+] {counter+1} of {(pages * tries)+1}: Processing timespan {new_lower_limit} - {new_upper_limit}")

                self.flickrquerier_obj = FlickrQuerier(self.project_name,
                                           self.area_name,
                                           bbox=self.bbox,
                                           text_search=self.text_search,
                                           tags=self.tags,
                                           tag_mode=self.tag_mode,
                                           has_geo=self.has_geo,
                                           textual_results_to_return=self.textual_results_to_return,
                                           perform_textual_search=self.perform_textual_search,
                                           min_upload_date=new_lower_limit,
                                           max_upload_date=new_upper_limit,
                                           accuracy=self.accuracy,
                                           toget_images=self.toget_images,
                                           image_size=self.image_size,
                                           api_creds_file=self.api_credentials_path,
                                           allowed_licenses=self.allowed_licenses,
                                           subquery_status=True)

                if not self.flickrquerier_obj.toomany_pages[1] and self.flickrquerier_obj.unique_ids is not None:
                    all_unique_ids = all_unique_ids.union(self.flickrquerier_obj.unique_ids)
                    '''
                    instead of id's we collect the result dict
                    '''
                    final_result_dict_list.append(self.flickrquerier_obj.result_dict)
                    if perform_textual_search:
                        if len(all_unique_ids) > textual_results_to_return:
                            print(f'[*] textual search: {textual_results_to_return} posts fetched. Finishing search.')
                            break
                else:
                    print("--" * 30)
                    print('[!] CAUTION: At least one subquery still returned too many results.')
                    print('[*] Further adjusting timespan...')
                    break
                #assign new timespan boundaries for the next iteration
                lower_limit_timespan = upper_limit_timespan = new_upper_limit
            '''
            If the execution reached this point, all the gathered unique ids of all subqueries will 
            be used to produce one CSV file.
            '''
            print("#-" * 30)
            print("[+] Successfully acquired all unique ids of subquery")
            print(f"[*] Querying a total of {len(all_unique_ids)} ids and writing to csv file..")
            self.flickrquerier_obj.write_info(final_result_dict_list)
            print("--" * 30)
            print(f"[+] Acquiring metadata - done.")
            print("--" * 30)
            if self.toget_images:
                print(f"[*] Downloading images..")
                self.flickrquerier_obj.get_images(final_result_dict_list)
                print("\n")
                print("--" * 30)
                print(f"[+] Download images - done.")
                print("--" * 30)
            print("--" * 30)
            print("[+] FlickrQuerier Class - done")
            break

    def geojson_to_bbox(self, file_):
        '''
        Create flickr query bounding boxes
        from the first [0] and third [2] element in a geojson file

        flickr bbox format: e.g. ['9.413564,47.282421,9.415497,47.285627']
        (list with containing 1 string with comma separated coordinates of the lower left and opper right corner)
        :param file:
        :return:
        '''
        bbox_list = []
        with open(file_) as f:
            data = json.load(f)

        for feature in data['features']:
            bbox_data = {'bbox': None,
                         'name': None}

            coordinates = feature['geometry']['coordinates'][0]
            name = feature['properties']['Name']
            # need element 0 nad 2
            lowerleft = coordinates[0]
            upperright = coordinates[2]
            bbox = [f"{lowerleft[0]},{lowerleft[1]},{upperright[0]},{upperright[1]}"]
            bbox_data['bbox'] = bbox
            bbox_data['name'] = name
            bbox_list.append(bbox_data)
        return bbox_list

    ############################################################################################
    ############################################################################################

    def body(self):
        #check if project directory exists
        if not os.path.isdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.project_name)):
            print('[*] Creating project folder...')
            os.mkdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.project_name))
        #check of geojson has to be parsed
        if self.geojson_file is not None:
            print("[*] Parsing GeoJson file. Extracting contained bounding boxes...")
            for bbox_data in self.geojson_to_bbox(self.geojson_file):
                self.area_name = bbox_data['name']
                if self.area_name not in already_processed:
                    '''
                    only break out of this loop if less then 15 pages are returned either from the initial
                    FlickrQuerier or the following sub-queries of the big_box_handler function.
                    '''
                    print("**" * 30)
                    print(f"[*] Processing new area: {self.area_name}")
                    print("**" * 30)
                    self.flickrquerier_obj = FlickrQuerier(self.project_name,
                                               self.area_name,
                                               bbox=bbox_data['bbox'],
                                               text_search=self.text_search,
                                               tags=self.tags,
                                               tag_mode=self.tag_mode,
                                               has_geo=self.has_geo,
                                               textual_results_to_return=self.textual_results_to_return,
                                               perform_textual_search=self.perform_textual_search,
                                               min_upload_date=self.min_upload_date,
                                               max_upload_date=self.max_upload_date,
                                               accuracy=self.accuracy,
                                               toget_images=self.toget_images,
                                               image_size=self.image_size,
                                               api_creds_file=self.api_credentials_path,
                                               allowed_licenses=self.allowed_licenses,
                                               subquery_status=False)
                    '''
                    Check if flickr_obj.toomany_pages is True 
                    which means sub-queries with smaller timespan need to be initiated
                    '''
                    if self.flickrquerier_obj.toomany_pages[1]:
                        self.high_data_volume_handler(bbox_data['bbox'], self.flickrquerier_obj.toomany_pages[0])

                else:
                    print("##" * 30)
                    print(f"[!] Already processed area: {self.area_name}")
                    print("##" * 30)

        #else query the single bounding box
        elif self.bbox is not None or self.text_search is not None:
            if self.bbox is not None:
                print("[*] Parsing single bounding box...")
            if self.text_search is not None:
                print("[*] Performing textual search by free text...")
            if self.tags is not None:
                print("[*] Performing textual search by tags..")

            self.area_name = '{}_{:%m_%d_%H_%M_%S}'.format(self.project_name, datetime.datetime.now())

            self.flickrquerier_obj = FlickrQuerier(self.project_name,
                                       self.area_name,
                                       bbox=self.bbox,
                                       text_search=self.text_search,
                                       tags=self.tags,
                                       tag_mode=self.tag_mode,
                                       has_geo=self.has_geo,
                                       textual_results_to_return=self.textual_results_to_return,
                                       perform_textual_search=self.perform_textual_search,
                                       min_upload_date=self.min_upload_date,
                                       max_upload_date=self.max_upload_date,
                                       accuracy=self.accuracy,
                                       toget_images=self.toget_images,
                                       image_size=self.image_size,
                                       api_creds_file=self.api_credentials_path,
                                       allowed_licenses=self.allowed_licenses,
                                       subquery_status=False)

            '''
            Check if flickr_obj.toomany_pages is True 
            which means sub-queries with smaller timespan need to be initiated
            '''
            if self.flickrquerier_obj.toomany_pages[1]:
                self.high_data_volume_handler(self.flickrquerier_obj.toomany_pages[0], self.textual_results_to_return, self.perform_textual_search)

##########################################################################################
if __name__ == '__main__':
    '''change project name!!!'''
    project_name = 'text_search_has_geo_redkite' # KT_ZG_SZ

    path_CREDENTIALS = "FLICKR_API_KEY.txt"

    # FOR GEOSPATIAL QUERIES SET BOUNDING BOX
    KT_ZG_SZ = ['8.387433, 46.883662, 9.010485, 47.248475']
    KT_ZG = ['8.390165, 47.079007, 8.701964, 47.247543']
    red_kite_around_argaty = ['-4.063436, 56.190326, -3.956491, 56.231234']
    # birds_around_lochwinnoch = ['-4.619658, 55.789749, -4.588323, 55.806828']
    chiltern_hills_bbox = ['-1.152104, 51.47025, -0.432355, 51.890047']

    # THERE ARE 2 WAYS FOR TEXTUAL SEARCHES: (1) BY TAGS AND TAG_MODE, (2) BY FREE TEXT IN TITLE, DESCRIPTION AND TAGS
    textual_results_to_return = 999999999 # the amount of posts to query with textual search (either tags or free text)
    # TAGS SEARCH AND TAG MODE
    tag_mode = 'any' # either 'all' (AND) or 'any' (OR)
    tags = ['red kite, rotmilan']
    # FREE TEXT SEARCHES: THEREFORE TITLE, DESCRIPTION AND TAGS ARE INCLUDED AND SCREENED!
    red_kite_text_search = 'red kite'
    # EITHER BBOX OR SEARCH_TEXT HAS TO BE NONE, NOT BOTH CAN BE SUPPLIED!
    flickrframe_obj = FlickrFrame(project_name,
                            path_CREDENTIALS,
                            bbox=None, #KT_ZG_SZ
                            text_search=red_kite_text_search,
                            tags=None,
                            tag_mode=None,
                            has_geo=True, # for textual search, if images should have geocoordinates
                            textual_results_to_return=textual_results_to_return,
                            allowed_licenses='all', #'3,4,5'
                            min_upload_date=None, #None
                            max_upload_date=None, #None,
                            toget_images=True,
                            image_size='medium') # 'small', 'medium', 'large', 'original'