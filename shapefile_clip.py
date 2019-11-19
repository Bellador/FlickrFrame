import os
import re
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

def load_shps():
    '''
    load the shapefiles into a dictionary with the key being the corresponding region name
    :return: shapes_bin
    '''
    print('Loading shapefiles...')
    shapes_bin = {}

    shapes = os.listdir(shapefile_path)

    for shape in shapes:
        if shape.endswith('.shp'):
            region_name = shape[:-4] ###CHANGE THIS TO CORRECTLY CLIP INPUT WITH CORRESPONDING SHAPEFILE
            print(f"loading shapefile for region {region_name}")
            full_path = os.path.join(shapefile_path, shape)
            shape_gdf = gpd.read_file(full_path)
            # print(shape_gdf.head())
            #check if the shapefile is in the defined common crs
            if shape_gdf.crs['init'] == common_crs:
                shapes_bin[region_name] = shape_gdf
            #otherwise convert to common crs
            else:
                print(f'crs {shape_gdf.crs} is different from common crs {common_crs}')
                shape_gdf['geometry'] = shape_gdf['geometry'].to_crs(epsg=common_crs)
    return shapes_bin

def read_csv_to_gdf(file_path):
    df = pd.read_csv(file_path, delimiter=";") #, usecols=['lat', 'lng']
    # print(df.head)
    gdf = gpd.GeoDataFrame(df, crs={'init': 'epsg:4326'}, geometry=[Point(xy) for xy in zip(df.lng, df.lat)])
    '''
    finde the region name inside the file_path of fullowing structure: 'metadata_REGION_data'
    '''
    region_pattern = r'metadata_([^_]+)_'
    region_name = re.search(region_pattern, file_path).group(1)
    return region_name, gdf

def clip_shp(gdf, shp_):
    '''

    :param gdf: input data that will be clipped
    :param shp_: shapefile used as mask for clipping
    :return: clipped dataframe
    '''
    mask = shp_.geometry.unary_union
    gdf_clipped = gdf[gdf.geometry.intersects(mask)]
    return gdf_clipped

if __name__ == '__main__':
    '''
    Purpose: Clip the bounding box query returns from the FlickrAPI and the YFCC100M database

    1. Iterate over folders in workspace (CC folder, FlickrAPI folder)
    2. Clip each file with the corresponding shapefile (determined by region name!) 

    '''
    common_crs = 'epsg:4326'
    workspace = "C:/Users/mhartman/PycharmProjects/FlickrFrame"
    shapefile_path = "C:/Users/mhartman/PycharmProjects/Ross_query/area_shapefile/split_shapefiles_by_attribute/shps_500m_buffer"
    # loads all needed shapefiles at once
    shapes_bin = load_shps()
    print("-" * 30)
    #create two new directories for the clipped FlickrAPI and YFCC100M data
    try:
        os.mkdir(os.path.join(workspace, 'flickrAPI_clipped'))
    except FileExistsError:
        print("Outputfolder for clipped Flickr data exists already.\n Continue.")
    try:
        os.mkdir(os.path.join(workspace, 'yfcc100m_clipped'))
    except FileExistsError:
        print("Outputfolder for clipped database data exists already.\n Continue.")
    flickrAPI_clipped = os.path.join(workspace, 'flickrAPI_clipped')
    yfcc100m_clipped = os.path.join(workspace, 'yfcc100m_clipped')
    # test_path = "C:/Users/mhartman/PycharmProjects/Ross_query/data/from_YFCC100M_db/metadata_CH_FM_2019_10_24.csv"

    #walk through the directories
    for (root, dirs, files) in os.walk(workspace, topdown=True):
        #iterating over both FlickrAPI and CC boundingbox folders
        if re.search(r'from_FLICKR_API', root): #re.search(r'from_FLICKR_API', root) or
            for file in files:
                region, gdf = read_csv_to_gdf(os.path.join(root, file))
                gdf_clipped = clip_shp(gdf, shapes_bin[region])
                #convert back into pandas dataframe and dropping the geometry column again
                df_clipped = pd.DataFrame(gdf_clipped.drop(columns='geometry'))
                #prepare the saving of clipped dataframe
                file_name = file[:-4] + '_cropped.csv'
                if re.search(r'from_FLICKR_API', root):
                    save_path = os.path.join(workspace, flickrAPI_clipped, file_name)
                elif re.search(r'from_YFCC100M_db', root):
                    save_path = os.path.join(workspace, yfcc100m_clipped, file_name)
                df_clipped.to_csv(save_path)
                print(f"Saving clipped csv for region: {region}")
            print(f'--------------------finished: {root}----------------------------------')