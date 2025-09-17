import os
import argparse
import datetime
import json
import joblib
import pandas as pd

import config

import utils


N_JOBS = 2

VAR_PREC = 'total_precipitation'
VAR_TEMP = '2m_temperature'


VALID_VARIABLES = {VAR_PREC, VAR_TEMP}

VAR_FOLDERNAME = {
    VAR_PREC: config.DATA_PREC_FOLDERPATH,
    VAR_TEMP: config.DATA_TEMP_FOLDERPATH
}

VAR_PREFIX = {
    VAR_PREC: 'total-precipitation',
    VAR_TEMP: '2m-temperature',
}


def download(
    var : str,
    date : datetime.datetime,
    download_filepath : str,
    overwrite : bool = False,
    url : str = None,
    key : str = None,
    exist_ok : bool = True,
    # request_full_month:bool = False,
):
    if var == VAR_PREC:
        utils.download_total_precipitation(
            date = date,
            download_filepath = download_filepath,
            overwrite = overwrite,
            url = url,
            key = key,
            request_full_month = True,
            exist_ok = exist_ok,
        )
    elif var == VAR_TEMP:
        utils.download_mean_temperature(
            date = date,
            download_filepath = download_filepath,
            overwrite = overwrite,
            url = url,
            key = key,
            request_full_month = True,
            exist_ok = exist_ok,
        )
    else:
        raise NotImplementedError(f'var = {var}')


def get_dates(
    startdate:datetime.datetime,
    enddate:datetime.datetime,
):
    startyear = startdate.year
    startmonth = startdate.month
    endyear = enddate.year
    endmonth = enddate.month

    dates = []
    
    for year in range(startyear, endyear + 1):
        month = startmonth if year == startyear else 1
        stopmonth = endmonth if year == endyear else 12
        while month <= stopmonth:
            dates.append(datetime.datetime(year, month, 1))
            month += 1

    return dates


def get_download_filepath(
    folderpath:str,
    var:str,
    date:datetime.datetime,
):
    return os.path.join(
        folderpath, 
        VAR_PREFIX[var], 
        f"{VAR_PREFIX[var]}_{date.strftime('%Y-%m')}.nc",
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog = 'python scripts/download_multiple.py',
        description = (
            'Script to ERA5 data daily single values in monthly chunks '
            '-- daily mean temperature, daily total precipitation -- '
            'from given start year and month to end year and month.'
        ),
    )

    parser.add_argument('-c', '--credentials', action='store', required=True, help="filepath to json file with CDSAPI credentials")
    parser.add_argument('-s', '--startdate', action='store', required=True, help="[YYYY-MM-DD] Start date to download the variable for. Day is just a placeholder, the whole month would be downloaded.")
    parser.add_argument('-e', '--enddate', action='store', required=True, help="[YYYY-MM-DD] End date to download the variable for. Day is just a placeholder, the whole month would be downloaded.")
    parser.add_argument('-v', '--var', action='append', required=True, help="[options: '2m_temperature' 'total_precipitation'] ERA5 variable to download.")
    parser.add_argument('-F', '--folderpath', action='store', required=True, help="folderpath where the file is to be downloaded to. Overriden by -f.")
    # parser.add_argument('-m', '--request-full-month', action='store_true', help="Download full month.")

    args = parser.parse_args()

    with open(args.credentials, 'r') as h:
        credentials = json.load(h)
    
    URL = credentials['url']
    KEY = credentials['key']

    startdate = datetime.datetime.strptime(args.startdate, '%Y-%m-%d')
    enddate = datetime.datetime.strptime(args.enddate, '%Y-%m-%d')

    dates = get_dates(startdate=startdate, enddate=enddate)

    download_folderpath = args.folderpath

    for var in args.var:
        if var not in VALID_VARIABLES:
            raise ValueError(f'Invalid var={var}. var must be from {VALID_VARIABLES}.')

    jobs = [
        (   
            var,
            date,
            get_download_filepath(
                folderpath = download_folderpath,
                var = var,
                date = date,
            ),
            False, # overwrite
            URL,
            KEY,
            True, # exist_ok
        )
        for var in args.var for date in dates
    ]


    catalog_filepath = os.path.join(download_folderpath, 'catalog.csv')
    if os.path.exists(catalog_filepath):
        response = input("Warning: In the current implementation, the catalog.csv would be replaced. Do you still wish to continue? [Y/n]: ")
        if response.lower() in ['yes', 'y']:
            pass
        elif response.lower() in ['n', 'n']:
            print('Bonne journee ~')
            exit(0)
        else:
            raise ValueError('Invalid response. Please reply either Y/n.')


    joblib.Parallel(n_jobs=N_JOBS, prefer="threads", verbose=10)(
        joblib.delayed(download)(_var, _date, _filepath, _overwrite, _url, _key, _exist_ok) 
        for _var, _date, _filepath, _overwrite, _url, _key, _exist_ok in jobs
    )


    data = {
        'var': [],
        'date': [],
        'filepath': [],
    }

    for var, date, filepath, _, _, _, _ in jobs:
        data['var'].append(var)
        data['date'].append(date)
        data['filepath'].append(os.path.abspath(filepath))
    
    # To do : If I add more dates to download, append the catalog.csv rather than overrwrite it.

    pd.DataFrame(data=data).to_csv(catalog_filepath, index=False)
    