import requests, util, os, time, subprocess, sys, random, string
from bs4 import BeautifulSoup

# logging
from loguru import logger as log

log.remove(0)
log.add(sys.stderr, format = "<level>[{time:DD-MMM-YYYY HH:mm:ss}]</level> {message}",
        backtrace = True, diagnose = True, colorize = True, level = 5)
log.add('log.txt', format = "[{time:DD-MMM-YYYY HH:mm:ss}] {message}",
        backtrace = True, diagnose = True, colorize = True, level = 5)

trace, info, err, succ = (log.trace, log.info, log.error, log.success)


EXTS = tuple(util.audio_exts + util.image_exts + util.video_exts)
ARIA2_FILENAME = ''.join(random.choice(string.ascii_letters) for x in range(10)) + '.txt'

aria2c_args = [
    'aria2c',
    '--max-connection-per-server=5',
    '--max-concurrent-downloads=5',
    '--auto-file-renaming=false',
    '--remote-time=true',
    '--log-level=error',
    '--console-log-level=error',
    '--download-result=hide',
    '--summary-interval=0',
    '--file-allocation=none',
    '--continue=true',
    '--check-certificate=false',
    '--quiet=true',
    '-Z'
]

def dump_thread(link, board_sfx):
    img_links = []

    while True:
        try:
            r = requests.get(link)
            break
        except Exception as e:
            err(f"dump_thread: {e}")

    soup = BeautifulSoup(r.text, "html.parser")
    htm_links = [x.get('href') for x in soup.find_all("a") if x.get('href') is not None]
    
    for htm in htm_links:
        if not htm.startswith(f'/{board_sfx}/src/'):
            continue

        if not htm.endswith(EXTS): # /azu/src/1316779210367.jpg
            continue

        img_links.append('http://ii.yakuji.moe' + htm)
    
    img_links = list(set(img_links)) # remove duplicates
    img_links.append(link)
    subprocess.run(aria2c_args + img_links)

def dump(_url, _from, _to):
    if 'html' in _url: # 'http://ii.yakuji.moe/azu/5.html'
        _url = os.path.dirname(_url) # 'http://ii.yakuji.moe/azu'

    board_sfx = _url[_url.rfind("/")+1:] # azu
    _range = [x for x in range(_from, _to)]

    os.makedirs(board_sfx, exist_ok=True)
    os.chdir(board_sfx)

    soup = BeautifulSoup(requests.get(_url).text, "html.parser")
    
    page_sfx = ['index.html']
    for sp in soup.find_all("a"):
        if '.html' not in str(sp):
            continue
            
        if len(sp.get('href')) < 10: # 9999
            page_sfx.append(sp.get('href'))

    threads = []
    
    for sfx in page_sfx:
        if page_sfx.index(sfx) not in _range:
            continue
        
        while True:
            try:
                r = requests.get(f'{_url}/{sfx}')
                break
            except Exception as e:
                err(f"dump: {e}")

        soup = BeautifulSoup(r.text, "html.parser")
        htm_links = [x.get('href') for x in soup.find_all("a") if x.get('href') is not None] 

        for htm in htm_links:
            if htm.startswith('./res/') and htm.endswith('.html'): #'./res/10992.html'
                threads.append(_url + htm[1:])

        info(f'{page_sfx.index(sfx) + 1} / {len(page_sfx)}, {len(threads)} found', end = '\r')

    for thread in threads:
        num = thread[thread.rfind('/')+1:-5]
        os.makedirs(num, exist_ok=True)
        os.chdir(num)

        trace(f'{threads.index(thread) + 1} / {len(threads) }, {thread}', end = '      \n')
        dump_thread(thread, board_sfx)

        os.chdir('..')

    os.chdir('..')
    

if len(sys.argv) < 3:
    print(sys.argv[0], "startpage-endpage <link to board>")
    print(sys.argv[0], "0-5 http://ii.yakuji.moe/azu")
    sys.exit()

f, t = sys.argv[1].split('-')

for i in range(2, len(sys.argv)):
    dump(sys.argv[i], int(f), int(t))
