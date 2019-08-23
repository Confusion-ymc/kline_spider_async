#!coding:utf-8
import asyncio
import aiohttp
import aioredis
from aiosocksy import Socks5Auth
from aiosocksy.connector import ProxyConnector, ProxyClientRequest
import datetime
import pymongo

import pytz
from urllib3 import disable_warnings
import json
import time
from stem import Signal
from stem.control import Controller

disable_warnings()
exchange_info = {
    'url_map': {
        'BN': 'https://api.binance.com/api/v1/klines?symbol={symbol}&interval={interval}&startTime={start_time}&endTime={end_time}&limit=1000',
        # 'HB': 'https://api.huobi.pro/market/history/kline?period={interval}&size=2000&symbol={symbol}'
        'BF': 'https://api-pub.bitfinex.com/v2/candles/trade:{interval}:{symbol}/hist?start={start_time}&end={end_time}&limit=5000&sort=-1'
    },
}
interval_map = {
    'BN': {
        '1m': '1m',
        '5m': '5m',
        '15m': '15m',
        '30m': '30m',
        '1h': '1h',
        '1d': '1d'
    },
    'HB': {
        '1m': '1min',
        '5m': '5min',
        '15m': '15min',
        '30m': '30min',
        '1h': '60min',
        '1d': '1day'
    },
    'BF': {
        '1m': '1m',
        '5m': '5m',
        '15m': '15m',
        '30m': '30m',
        '1h': '1h',
        '1d': '1D'
    },
    'interval_min': {
        '1m': 1,
        '5m': 5,
        '15m': 15,
        '30m': 30,
        '1h': 60,
        '1d': 60 * 24
    }
}


class SessionMaker:
    def __init__(self):
        self.session = None
        self.auth = Socks5Auth(login='...', password='...')
        self.socks = proxy

    async def open_session(self):
        connector = ProxyConnector()
        self.session = aiohttp.ClientSession(connector=connector, request_class=ProxyClientRequest)
        await asyncio.sleep(0)
        # async with aiohttp.ClientSession(connector=connector, request_class=ProxyClientRequest) as session:
        #     self.session = session
        #     while self.task_finish != task_count:
        #         await asyncio.sleep(0)
        #     self.task_finish = 0

    async def close_session(self):
        if not self.session.closed:
            await self.session.close()


class SymbolKline:
    def __init__(self, exchange, trade_type, symbol_name, interval, start_time, stop_time, exchangeSymbolPair, mongodb,
                 redis_helper):
        self.redis_helper = redis_helper
        self.exchange = exchange
        self.mongodb = mongodb
        self.trade_type = trade_type
        self.symbol_name = symbol_name
        self.exchangeSymbolPair = exchangeSymbolPair
        self.interval = interval
        self.start_time = start_time - start_time % 60 * 1000
        self.req_count = 1000
        self.end_time = min(self.start_time + (self.req_count - 5) * 60 * 1000 * interval_map['interval_min'][self.interval],stop_time)
        self.stop_time = stop_time
        self.retry = True
        self.finish = False
        self.last_data_time = None

    def make_url(self):
        url = exchange_info['url_map'][self.exchange].format(start_time=self.start_time,
                                                             end_time=self.end_time if self.end_time < self.stop_time else self.stop_time,
                                                             interval=interval_map[self.exchange][self.interval],
                                                             symbol=self.exchangeSymbolPair if self.exchange != 'BF' else 't' + self.exchangeSymbolPair.upper())
        return url

    def make_next_start_time(self):
        self.start_time = self.end_time
        self.end_time = self.end_time + (self.req_count - 5) * 60 * 1000 * interval_map['interval_min'][self.interval]

    async def fetch(self, session_maker: SessionMaker, params=None, headers=None):
        url = self.make_url()
        # url = 'https://ifconfig.co/json'
        print('请求 {}'.format(url))
        res = ''
        if not headers:
            headers = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3'
            }
        try:
            async with session_maker.session.request(
                    proxy=session_maker.socks,proxy_auth=session_maker.auth,
                    method='GET',timeout=10,
                    url=url,params=params,
                    headers=headers,ssl=False) as response:
                res = await response.text()
                res = json.loads(res)
                assert type(res) is list
                if res:
                    await self.parse_data(res)

                redis_key = 'KlineEndTime:{exchange}:{trade_type}:{interval}:{symbol_name}'.format(
                    exchange=self.exchange,
                    trade_type=self.trade_type,
                    interval=self.interval,
                    symbol_name=self.symbol_name
                )
                await self.redis_helper.save_time(redis_key, self.end_time)
                self.retry = False
                if self.end_time >= self.stop_time:
                    self.finish = True
                else:
                    self.make_next_start_time()
                print('成功++ {}'.format(url))
        except Exception as e:
            # print(e)
            # print(res)
            print('失败-- {} {}'.format(url, self.symbol_name))
            self.retry = True
        await asyncio.sleep(0)

    def timesamp_to_datetime(self, time_stamp):
        if len(str(time_stamp)) >= 10:
            time_stamp = time_stamp / 1000
        date = datetime.datetime.utcfromtimestamp(time_stamp)
        date = date.replace(tzinfo=pytz.utc)
        return date
        # 返回北京时间
        # return date.astimezone(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')

    async def parse_data(self, data):
        # 从小到大整理顺序
        data.sort(key=lambda x: x[0])
        BaseSymbol, QuoteSymbol = self.symbol_name.split('.')[0].split('/')
        for item in data:
            # 过滤最后一条可能重复的数据
            if item[0] == self.last_data_time:
                # print('跳过 {} '.format(self.timesamp_to_datetime(item[0])))
                continue
            final_data = {
                'MoSymbol': self.symbol_name,
                "BaseSymbol": BaseSymbol,
                "QuoteSymbol": QuoteSymbol,
                "TradeType": self.trade_type,
                "Interval": self.interval,
                'DateTime': self.timesamp_to_datetime(item[0]),
                'Timestamp': item[0],
                'Open': item[1],
                'High': item[2],
                'Low': item[3],
                'Close': item[4],
                'Vol': item[5],
            }
            await self.mongodb.save_data(final_data, self.stop_time)
        if data:
            self.last_data_time = data[-1][0]
        # if BaseSymbol == 'BTC':
        #     print(self.timesamp_to_datetime(data[0][0]))
        #     print('...')
        #     print(self.timesamp_to_datetime(data[-1][0]))


class SymbolHelper:
    def __init__(self):
        self.url = 'https://platformd.matrixone.io/api/v2/PlatformSupportedExchangeSymbolPair/All'

        self.symbols = {}

    async def update_symbols_by_api(self, params=None, headers=None):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(
                            method='GET',
                            timeout=20,
                            url=self.url,
                            headers=headers,
                            ssl=False
                    ) as response:
                        res = await response.text()
                        res = json.loads(res)
                        print('成功++ {}'.format(self.url))
                        self.parse_api_data(res)
                        break
            except Exception as e:
                print('失败-- {}'.format(self.url))

    def parse_api_data(self, data):
        for item in data['result']['exchanges']:
            for symbol in item['symbolPairs']:
                mo_symbol, query_trade_type = self.trans_redis_key(symbol, item['name'])
                symbol.update({
                    'mo_symbol': mo_symbol
                })
            self.symbols[item['name']] = item['symbolPairs']
        print('更新symbols成功')

    def trans_redis_key(self, symbol, exchange):
        trade_type = symbol['tradeCategory']
        symbol_name = symbol['baseSymbol'] + '/' + symbol['quoteSymbol']
        if trade_type == 'Perpetual':
            query_trade_type = 'Contracts'
            query_symbol = symbol_name + '.' + exchange
        elif trade_type == 'Future':
            query_trade_type = 'Future'
            if not symbol['contractCategory']:
                query_symbol = symbol['exchangeSymbolPair']
            else:
                query_symbol = symbol_name + '.' + exchange + '.' + symbol['contractCategory']
        else:
            query_trade_type = 'Spot'
            query_symbol = symbol_name + '.' + exchange
        return query_symbol, query_trade_type

    def get_symbols_by_trade_type(self, exchange, trade_type):
        res = [item for item in self.symbols[exchange] if
               item['tradeCategory'] == self.trans_trade_type_to_mo(trade_type, True)]
        return res

    def trans_trade_type_to_mo(self, trade_type, reverse=False):
        trade_type_map = {
            'Spot': 'Spot',
            'Margin': 'Margin',
            'Perpetual': 'Contracts',
            'Future': 'Future'
        }
        if reverse:
            trade_type_map = {v: k for k, v in trade_type_map.items()}
        return trade_type_map[trade_type]


class MongodbHelper:
    def __init__(self, exchange, redis_helper):
        self.conn = pymongo.MongoClient('mongodb://{host}:{port}'.format(host='localhost', port=27017),
                                        socketTimeoutMS=60000)
        self.exchange = exchange
        self.my_set = eval('self.conn.{}.Kline'.format(self.exchange))
        self.last_time_dic = {}
        self.save_cache = []
        self.execute_count = 500
        self.redis_helper = redis_helper

    async def save_data(self, data, stop_time):
        # 判断是否在停止时间之前
        if data['Timestamp'] < stop_time:
            self.save_cache.append(data)

        if len(self.save_cache) >= self.execute_count:
            await self.execute_save()

    async def execute_save(self):
        # start_time = time.time()
        if self.save_cache:
            self.my_set.insert_many(self.save_cache)

            redis_key = 'KlineEndTime:{exchange}:{trade_type}:{interval}:{symbol_name}'.format(
                exchange=self.exchange,
                trade_type=self.save_cache[-1]['TradeType'],
                interval=self.save_cache[-1]['Interval'],
                symbol_name=self.save_cache[-1]['MoSymbol']
            )
            await self.redis_helper.save_time(redis_key, self.save_cache[-1]['Timestamp'])
            # print('mongodb 写入成功 {} 条数据, 耗时: {}'.format(len(self.save_cache), time.time() - start_time))
            self.save_cache = []

    def query_last_timestamp(self, symbol, trade_type, interval):
        query_str = {"Symbol": symbol, 'TradeType': trade_type, "Interval": interval}
        return self.my_set.find_one(query_str, sort=[("UTCTimeStamp", -1)])['UTCTimeStamp']


class AsyncRedisHelper:
    def __init__(self):
        self.pool = None

    async def make_pool(self):
        self.pool = await aioredis.create_pool(
            'redis://192.168.106.217',
            minsize=5, maxsize=10)

    async def get_data(self, key):
        with await self.pool as conn:  # low-level redis connection
            val = await conn.execute('get', key)
            return int(val.decode()) if val else None

    async def save_time(self, key, end_time):
        with await self.pool as conn:  # low-level redis connection
            await conn.execute('set', key, end_time)

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()


async def main():
    symbol_helper = SymbolHelper()

    redis_helper = AsyncRedisHelper()
    session_maker = SessionMaker()
    await session_maker.open_session()
    await symbol_helper.update_symbols_by_api()
    await redis_helper.make_pool()

    for interval in ['1d', '1h', '5m', '30m', '15m', '1m']:
        exchanges = ['BN', 'BF']
        all_klines = []
        MongodbHelperList = {}
        for exchange in exchanges:
            MongodbHelperList[exchange] = MongodbHelper(exchange, redis_helper)
            exhchange_kline = await make_all_kline(
                redis_helper=redis_helper,
                mongodb=MongodbHelperList[exchange],
                interval=interval,
                symbol_helper=symbol_helper,
                trade_type='Spot',
                exchange=exchange
            )
            all_klines.append(exhchange_kline)
        for_req_kline = []
        while True:
            for item in all_klines:
                if item:
                    for_req_kline += item[:count]
                    del item[:count]
            if for_req_kline:
                finish = False
                while not finish:
                    # change_proxy_flag = 0
                    tasks = []
                    for kline_item in for_req_kline:
                        if not kline_item.finish:
                            # change_proxy_flag += 1
                            tasks.append(kline_item.fetch(session_maker))
                    # req_count = len(tasks)
                    # # 切换代理
                    # if req_count and change_proxy_flag >= req_count / 2:
                    #     switch_tor_ip()
                    await asyncio.gather(*tasks)
                    print('')
                    finish = True
                    for kline_item in for_req_kline:
                        if not kline_item.finish:
                            finish = False
                print('{} {} 完成'.format(interval, [i.symbol_name for i in for_req_kline]))
                for_req_kline = []
            else:
                break
    for mongodb in MongodbHelperList.values():
        await mongodb.execute_save()
    await redis_helper.close()
    await session_maker.close_session()


# 发送切换IP请求
async def switch_tor_ip():
    with Controller.from_port(address='173.242.115.6', port=9051) as controller:
        while True:
            try:
                controller.authenticate()
                controller.signal(Signal.NEWNYM)
                print('切换代理中...')
                await asyncio.sleep(12)
                print('切换代理成功 ...')
            except:
                pass
            await asyncio.sleep(60)


# 创建Kline请求对象
async def make_all_kline(redis_helper, mongodb, interval, symbol_helper, trade_type, exchange):
    kline_obj_list = []
    if exchange not in exchange_info['url_map']:
        return kline_obj_list
    symbol_dict = symbol_helper.get_symbols_by_trade_type(exchange, trade_type)
    for symbol in symbol_dict:
        redis_key = 'KlineEndTime:{exchange}:{trade_type}:{interval}:{symbol_name}'.format(exchange=exchange,
                                                                                           trade_type=trade_type,
                                                                                           interval=interval,
                                                                                           symbol_name=symbol[
                                                                                               'mo_symbol'])
        start_time_ = await redis_helper.get_data(redis_key) or start_time
        if start_time_ >= stop_time:
            continue
        new_kline = SymbolKline(symbol_name=symbol['mo_symbol'],
                                exchangeSymbolPair=symbol['exchangeSymbolPair'],
                                trade_type=symbol_helper.trans_trade_type_to_mo(symbol['tradeCategory']),
                                interval=interval,
                                exchange=exchange,
                                start_time=start_time_,
                                stop_time=stop_time,
                                mongodb=mongodb,
                                redis_helper=redis_helper
                                )
        kline_obj_list.append(new_kline)
    return kline_obj_list


proxy = 'socks5://173.242.115.6:9050'  # 'http://173.242.115.6:8118'
# proxy = 'socks5://127.0.0.1:1080'  # 'http://173.242.115.6:8118'

count = 10

start_time = 1501516800000

# start_time = 1565381120000
stop_time = 1566316800000
# https://api-pub.bitfinex.com/v2/candles/trade:1m:tBTCUSD/hist?limit=1000&start=1523716800000&end=1523717800000&sort=-1
# https://api-pub.bitfinex.com/v2/candles/trade:1m:tBTCUST/hist?start=1523476800000&end=1523536800000&limit=5000
if __name__ == '__main__':
    tasks = [main(), switch_tor_ip()]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*tasks))
