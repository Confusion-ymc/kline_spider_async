#!coding:utf-8
import aiohttp, asyncio, time

pool = 100
timeout = 5
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
}

now = lambda :time.time()


async def make_session(params_list):
    sem = asyncio.Semaphore(pool)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for params, callback in params_list:
            task = loop.create_task(fetch_control_sem(params, session, sem, callback))
            # task.add_done_callback(callback)
            tasks.append(task)
        await asyncio.wait(tasks)


async def fetch_control_sem(params, session, sem, callback):  # 限制信号量
    async with sem:
        url = params[1]
        symbol = params[0]
        res = await fetch(url, session)
        await callback({'res': res, 'symbol': symbol})
        # return {'res': res, 'symbol': symbol}


async def fetch(url, session):
    try:
        async with session.get(url, timeout=timeout, headers=headers) as response:
            if response.status == 200:
                # return await response.text()
                return True
            else:
                return False
    except:
        return False


class KlineData:
    def __init__(self, trade_type, exchange):
        self.trade_type = trade_type
        self.exchange = exchange
        self.symbols = self.make_symbols()
        self.base_url = 'https://www.baidu.com'
        self.interval_dic = {
            '1m': '1m',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
        }
        self.not_finish_urls = [
            ['BTC/USDT.OK_'+str(i), 'https://www.baidu.com'] for i in range(500)

        ]

    def make_symbols(self):
        return {}

    def make_url(self, interval):
        for mo_symbol, exchange_symbol in self.symbols.items():
            url = self.base_url.format(symbol_name=exchange_symbol, interval=self.interval_dic[interval])
            self.not_finish_urls.append([mo_symbol, url])

    async def callback(self, y):
        # print(y.result())
        print(y)
        await asyncio.sleep(0)
loop = asyncio.get_event_loop()
while True:
    start = now()
    OK_KlineData = KlineData(trade_type='Spot', exchange='OK')
    tasks = []
    for params in OK_KlineData.not_finish_urls:
        tasks.append((params, OK_KlineData.callback))
    loop.run_until_complete(make_session(tasks))
    print('用时: {}'.format(now() - start))
    time.sleep(5)
