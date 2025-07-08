
import pandas as pd
import datacube
import geopandas as gpd
import argparse

# module load ard-pipeline/20250619-1012

parser = argparse.ArgumentParser(description='Collection completeness dataset counter')
parser.add_argument('--platform', type=str, help='landsat-8, landsat-7, landsat-5')
parser.add_argument('--date_from', type=str, help='2014-01-01')
parser.add_argument('--date_to', type=str, help='2015-01-01')
parser.add_argument('--out', type=str, help='geojson file path output')

args = parser.parse_args()

dc = datacube.Datacube()

platform = args.platform

date_from = args.date_from
date_to = args.date_to

f = open('/g/data/up71/projects/ARD_collection_completeness/scripts/landsat_scenes.txt')
a = f.readlines()
scenes = []
for i in a:
    scenes.append(i.replace('"','').replace('\n',''))


def do_counts(platform, scenes, date_from, date_to):
    data = {
        "platform": [],
        "scene_id": [],
        "level1": [],
        "level2": [],
        "completeness": [],
        "diff": [],
        "missing_level1": []
    }
    df = pd.DataFrame(data)
    if (platform == 'landsat-8'):
        level1_product_c1 = 'usgs_ls8c_level1_1'
        level1_product_c2 = 'usgs_ls8c_level1_2'
        level2_product = 'ga_ls8c_ard_3'
    elif (platform == 'landsat-7'):
        level1_product_c1 = 'usgs_ls7e_level1_1'
        level1_product_c2 = 'usgs_ls7e_level1_2'
        level1_product_ga_l1 = 'ga_ls7e_level1_3'
        level2_product = 'ga_ls7e_ard_3'
    elif (platform == 'landsat-5'):
        level1_product_c1 = 'usgs_ls5t_level1_1'
        #level1_product_c2 = 'usgs_ls5t_level1_2'
        level1_product_ga_l1 = 'ga_ls5t_level1_3'
        level2_product = 'ga_ls5t_ard_3'
    for scene in scenes:
        # perform level 1 count
        if (platform == 'landsat-8'):
            l1_c1_count = len(dc.find_datasets(product=level1_product_c1, region_code=scene, time=(date_from, date_to)))
            l1_c2_count = len(dc.find_datasets(product=level1_product_c2, region_code=scene, time=(date_from, date_to)))
            l1_count = l1_c1_count + l1_c2_count
        elif (platform == 'landsat-5'):
            l1_c1_count = len(dc.find_datasets(product=level1_product_c1, region_code=scene, time=(date_from, date_to)))
            l1_ga_count = len(dc.find_datasets(product=level1_product_ga_l1, region_code=scene, time=(date_from, date_to)))
            l1_count = l1_c1_count + l1_ga_count
        elif (platform == 'landsat-7'):
            l1_c1_count = len(dc.find_datasets(product=level1_product_c1, region_code=scene, time=(date_from, date_to)))
            l1_ga_count = len(dc.find_datasets(product=level1_product_ga_l1, region_code=scene, time=(date_from, date_to)))
            l1_count = l1_c1_count + l1_ga_count
        # perform level 2 count
        l2_count = len(dc.find_datasets(product=level2_product, region_code=scene, time=(date_from, date_to)))
        diff = l1_count - l2_count
        try:
            completness = l2_count/l1_count
        except ZeroDivisionError:
            completness = float('nan')
        if l2_count == 0 and l1_count >= 1:
            completness = 0
        if (l2_count > 0 and l1_count == 0):
            missing_level1 = 'true'
        else:
            missing_level1 = 'false'
        data = {
            "platform": [platform],
            "region_code": [scene],
            "level1": [l1_count],
            "level2": [l2_count],
            "completeness": [completness],
            "diff": [diff],
            "missing_level1": [missing_level1]
        }
        df_new_rows = pd.DataFrame(data)
        df = pd.concat([df, df_new_rows])
    return df

footprints_fpath = '/g/data/up71/projects/ARD_collection_completeness/vector/landsat_wrs2_descending.geojsonl'
df_ls_wrs2 = gpd.read_file(footprints_fpath)

df = do_counts(platform, scenes, date_from, date_to)

merged_df = df_ls_wrs2.merge(df, on="region_code", how="left")

clean_df = merged_df.dropna(thresh=4)

clean_df.to_file(args.out, driver="GeoJSON")
