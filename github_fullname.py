# -*- coding:utf-8 -*-

from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import string
from github import Github
import psycopg2
import time
import pandas as pd
import gc
from tqdm import tqdm

from sqlalchemy import create_engine
from sqlalchemy import types as sqltype
from config.database import HOST, PORT, USER, PASSWORD, DATABASE

psql_engine = create_engine("postgresql://"+USER+":"+PASSWORD+"@"+HOST+":"+str(PORT)+"/"+DATABASE)


def escape(s):
    return (s).replace("'", "''").replace('%', '%%').replace('<','').replace('>','')

def get_url_page(url, waitsec=0):
    time.sleep(waitsec)
    try:
        url = urllib.parse.quote(url, safe= string.printable)
        page = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        print("Error in HTTP request:", e)
        if e.code == 404:
            print("Error! Not found! ", e.code)
            return "404"
        elif e.code == 406:
            print("Error! Not Acceptable url! ", e.code)
            return "406"
        elif waitsec == 0 and e.code == 429:
            print("Wait 3min "+url)
            page = get_url_page(url, 180)
            return page
        elif waitsec <=300 and e.code == 429:
            print("Wait 30min "+url)
            page = get_url_page(url, 1800)
            return page
    return page


def getuserinfo_pygithub(pyg, username):
    user = pyg.get_user(username)
    fullname = user.name
    trueemail = user.email

    return fullname, trueemail

def getuserinfo_b4soup(urlpage):
    page = get_url_page(urlpage)
    if page == "404":
        return "404"
    elif page == "406":
        return "406"
    else:
        soup = BeautifulSoup(page, 'html.parser')
        fullnamecard = soup.find('span', attrs={'class': 'vcard-fullname'})
        if fullnamecard == None :
            return None
        fullname = fullnamecard.string.encode('utf-8').decode('utf-8').replace('\n','')
        #print("Fullname:", fullname)
        #print(fullname.split())
        final = ""
        for name in fullname.split():
            final=final+' '+name
        #print("Finalname:",final[1:])
        return final[1:]
        #return fullname



with psql_engine.connect() as conn:
    df = pd.read_sql("SELECT aliase_id, mailaddress FROM aliase WHERE source LIKE 'Github' AND personname IS NULL ;", con= conn)

github_ids = df['aliase_id'].values

for username in tqdm(github_ids[::-1]):
    url = "https://github.com/"+username
    try:
        fullname = getuserinfo_b4soup(url)
        if fullname == '' :
            continue
        if fullname == None :
            sql = """ UPDATE aliase SET personname=NULL WHERE aliase_id='%s'""" %(escape(username))
        else:
            sql = """ UPDATE aliase SET personname='%s' WHERE aliase_id='%s'""" %(escape(fullname), escape(username))
            
        with psql_engine.connect() as conn:
            conn.execute(sql.encode('utf-8').decode('utf-8'))
    except BaseException as err:
        print("Error in Updating:", err)
        #print(sql)
        continue
