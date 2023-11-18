import argparse
import datetime
import pandas as pd
from utils import perform_get_request, xml_to_load_dataframe, xml_to_gen_data, split_date_range
from tqdm import tqdm
from icecream import ic

def get_load_data_from_entsoe(regions, periodStart='202302240000', periodEnd='202303240000', output_path='./data'):
    
    # TODO: There is a period range limit of 1 year for this API. Process in 1 year chunks if needed
    years = split_date_range(datetime.datetime(2021, 1, 1), datetime.datetime(2022, 1, 1), datetime.timedelta(days=365))
    
    # URL of the RESTful API
    url = 'https://web-api.tp.entsoe.eu/api'

    # General parameters for the API
    # Refer to https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_documenttype
    params = {
        'securityToken': '1d9cd4bd-f8aa-476c-8cc1-3442dc91506d',
        'documentType': 'A65',
        'processType': 'A16',
        'outBiddingZone_Domain': 'FILL_IN', # used for Load data
        'periodStart': None, # in the format YYYYMMDDHHMM
        'periodEnd': None # in the format YYYYMMDDHHMM
    }

    dfs = []

    # Loop through the regions and get data for each region
    print(f"Fetching data for Load, periodStart={periodStart}, periodEnd={periodEnd}, regions={len(regions)}...")
    with tqdm(total=len(years)*len(regions)) as pbar:
        for start_date, end_date in years:
            ic(start_date, end_date)
            params['periodStart'] = start_date.strftime('%Y%m%d%H%M')
            params['periodEnd'] = end_date.strftime('%Y%m%d%H%M')
            for region, area_code in regions.items():
                
                params['outBiddingZone_Domain'] = area_code
            
                # Use the requests library to get data from the API for the specified time range
                response_content = perform_get_request(url, params)

                # Response content is a string of XML data
                data = xml_to_load_dataframe(response_content)
                data['Region'] = region

                # df = pd.concat([df, data])
                dfs.append(data)

                pbar.update(1)

    df = pd.concat(dfs)
    if not df.empty:
        df.to_csv(f'{output_path}/load.csv', index=False)
    else:
        print('LoadLoader: No data fetched')
        
    return

def get_gen_data_from_entsoe(regions, periodStart='202302240000', periodEnd='202303240000', output_path='./data'):
    
    # TODO: There is a period range limit of 1 day for this API. Process in 1 day chunks if needed
    days = split_date_range(datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 2), datetime.timedelta(days=1))
    # URL of the RESTful API
    url = 'https://web-api.tp.entsoe.eu/api'
    
    # General parameters for the API
    params = {
        'securityToken': '1d9cd4bd-f8aa-476c-8cc1-3442dc91506d',
        'documentType': 'A75',
        'processType': 'A16',
        'outBiddingZone_Domain': 'FILL_IN', # used for Load data
        'in_Domain': 'FILL_IN', # used for Generation data
        'periodStart': None, # in the format YYYYMMDDHHMM
        'periodEnd': None # in the format YYYYMMDDHHMM
    }
    dfs = []

    # Loop through the regions and get data for each region
    with tqdm(total=len(days)*len(regions)) as pbar:
        for start_date, end_date in days:
            params['periodStart'] = start_date.strftime('%Y%m%d%H%M')
            params['periodEnd'] = end_date.strftime('%Y%m%d%H%M')
            for region, area_code in regions.items():
                params['outBiddingZone_Domain'] = area_code
                params['in_Domain'] = area_code
            
                # Use the requests library to get data from the API for the specified time range
                response_content = perform_get_request(url, params)

                # Response content is a string of XML data
                rdfs = xml_to_gen_data(response_content)

                # Save the dfs to CSV files
                for _, rdf in rdfs.items():
                    # Save the DataFrame to a CSV file
                    rdf['Region'] = region
                    dfs.append(rdf)
                pbar.update(1)
    df = pd.concat(dfs) 
    if not df.empty:
        df.to_csv(f'{output_path}/gen.csv', index=False)
    else:
        print('GenLoader: No data fetched')
    return


def parse_arguments():
    parser = argparse.ArgumentParser(description='Data ingestion script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--start_time', 
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), 
        default=datetime.datetime(2023, 1, 1), 
        help='Start time for the data to download, format: YYYY-MM-DD'
    )
    parser.add_argument(
        '--end_time', 
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), 
        default=datetime.datetime(2023, 1, 2), 
        help='End time for the data to download, format: YYYY-MM-DD'
    )
    parser.add_argument(
        '--output_path', 
        type=str, 
        default='./data',
        help='Name of the output file'
    )
    return parser.parse_args()

def main(start_time, end_time, output_path):
    
    regions = {
        'HU': '10YHU-MAVIR----U',
        'IT': '10YIT-GRTN-----B',
        'PO': '10YPL-AREA-----S',
        # 'SP': '10YES-REE------0',
        # 'UK': '10Y1001A1001A92E',
        # 'DE': '10Y1001A1001A83F',
        # 'DK': '10Y1001A1001A65H',
        # 'SE': '10YSE-1--------K',
        # 'NE': '10YNL----------L',
    }

    # Transform start_time and end_time to the format required by the API: YYYYMMDDHHMM
    start_time = start_time.strftime('%Y%m%d%H%M')
    end_time = end_time.strftime('%Y%m%d%H%M')

    # Get Load data from ENTSO-E
    get_load_data_from_entsoe(regions, start_time, end_time, output_path)

    # Get Generation data from ENTSO-E
    get_gen_data_from_entsoe(regions, start_time, end_time, output_path)

if __name__ == "__main__":
    args = parse_arguments()
    main(args.start_time, args.end_time, args.output_path)