import asyncio
import csv
import json
import random
from pathlib import Path
from urllib.parse import urlparse, urlencode

import httpx

# comment any unnecessary fields
ZILLOW_API_FIELDS = [
    'statusType',
    'unformattedPrice',
    'addressStreet',
    'addressCity',
    'addressState',
    'addressZipcode',
    'beds',
    'baths',
    'livingArea',
    'lotAreaValue',
    'lotAreaUnit',
]


def get_config():
    path = Path('config.json')

    if path.exists():
        with open(path.resolve()) as f:
            return json.load(f)

    return {
        'INTERVAL': 5,
    }


config = get_config()
INTERVAL = config.get('INTERVAL', 5)


def get_headers(**kwargs):
    kwargs['User-Agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.360"

    return kwargs


def get_work_url(
        search_term,
        page=1,
        options=None
):
    if options is None:
        options = config

    if page < 1:
        raise Exception('Can\'t pass a value less than 1 for page')

    work_url = 'https://www.zillow.com/search/GetSearchPageState.htm'
    work_url = urlparse(work_url)
    query_params = {
        "searchQueryState": {
            "pagination": {
                "currentPage": page,
            },
            "usersSearchTerm": search_term,
            "mapBounds": {
                "west": -70.84688986914063,
                "east": -69.76748313085938,
                "south": 41.487256594497296,
                "north": 42.10991049792515,
            },
            "regionSelection": [
                {
                    "regionId": 784023,
                    "regionType": 31,
                }
            ],
            "isMapVisible": False,
            "filterState": {
                "price": {
                    "min": options.get('price', {}).get('min', 1000000),
                    "max": options.get('price', {}).get('max', 1500000),
                },
                "monthlyPayment": {
                    "min": options.get('monthlyPayment', {}).get('min', 2400)
                },
                "sortSelection": {
                    "value": "globalrelevanceex",
                },
                "isForSaleByAgent": {
                    "value": options.get('IsForSaleByAgent', False),
                },
                "isForSaleByOwner": {
                    "value": options.get('isForSaleByOwner', False),
                },
                "isNewConstruction": {
                    "value": options.get('isNewConstruction', False),
                },
                "isForSaleForeclosure": {
                    "value": options.get('isForSaleForeclosure', False),
                },
                "isComingSoon": {
                    "value": options.get("isComingSoon", False),
                },
                "isAuction": {
                    "value": options.get("isAuction", False),
                },
                "isRecentlySold": {
                    "value": options.get('isRecentlySold', False),
                },
                "isAllHomes": {
                    "value": options.get("isAllHomes", False),
                }
            },
            "isListVisible": True,
        },
        "wants": {
            "cat1": [
                "listResults",
                "mapResults",
            ]
        },
        "requestId": random.randint(0, 100000000),
    }

    work_url = work_url._replace(query=urlencode(query_params))
    return work_url.geturl()


def parse_results(data):
    cat1 = data['cat1']

    search_results = cat1['searchResults']['listResults']
    search_list = cat1['searchList']

    pagination = search_list.get('pagination', {})
    next_url = pagination.get('nextUrl', None)

    per_page = search_list['resultsPerPage']
    total_count = search_list['totalResultCount']

    for i, result in enumerate(search_results):
        home_info = result['hdpData']['homeInfo']

        search_results[i]['livingArea'] = home_info.get('livingArea', None)
        search_results[i]['lotAreaValue'] = home_info.get('lotAreaValue', None)
        search_results[i]['lotAreaUnit'] = home_info.get('lotAreaUnit', None)

    return search_results, total_count, per_page, next_url,


def only_keys(obj, keys):
    return {
        key: val for key, val in obj.items() if key in keys
    }


def save_csv(obj_list):
    obj_list = obj_list.copy()

    should_right_header = not Path('data.csv').exists()

    with open('data.csv', 'a') as f:
        writer = csv.DictWriter(f, fieldnames=ZILLOW_API_FIELDS)

        if should_right_header:
            writer.writeheader()

        writer.writerows(map(lambda obj: only_keys(obj, ZILLOW_API_FIELDS), obj_list))


def save_db(obj_list):
    return save_csv(obj_list)


async def scrap(client, search_term, **kwargs):
    page = 1

    async def f():
        nonlocal page

        work_url = get_work_url(
            search_term,
            **kwargs,
            page=page
        )

        response = await client.get(work_url, headers=get_headers())
        print(response.status_code)

        if response.is_error:
            print(response.text)

        if response.is_success:
            print('Processing page {}'.format(page))
            data = response.json()
            results, total, per_page, next_url = parse_results(data)
            save_db(results)

            if next_url:
                await asyncio.sleep(INTERVAL)
                page += 1
                return await f()

            return total, per_page, next_url

    return await f()


if __name__ == '__main__':
    async def main():
        save_path = Path('data.csv')
        if save_path.exists():
            print('data.csv already exists, press any key to overwrite it')
            input()

        client = httpx.AsyncClient()
        await scrap(
            client,
            'Cape Cod, MA',
            sold=True,
            max_price=1000000,
            min_price=500000,
        )
        await client.aclose()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
