import logging
import pandas as pd
import re
import requests
import pickle
import time
from pyquery import PyQuery
from urllib.parse import urljoin, urlparse
from models.model_issue import Issue

# Should get author email here by libs

def time_parse(message_date):
    
    if message_date==None :
        return 0
    try:
        message_date = str(message_date)
    except BaseException as err:
        print(err, "Return timestamp as 0")
        return 0
    try:
        time_format = "%a %b %d %H:%M:%S %Y"
        tsp = time.strptime(message_date, time_format)
    except ValueError as e:
        try:
            time_format =  "%a %d %b %Y %H:%M:%S %z "
            tsp = time.strptime(message_date.split('(')[0].replace(",",""), time_format)
        except ValueError as e:
            try:
                time_format =  "%a %d %b %Y %H:%M:%S %z"
                tsp = time.strptime(message_date.split('(')[0].replace(",",""), time_format)
            except ValueError as e:
                try:
                    time_format =  "%d %b %Y %H:%M:%S %z"
                    tsp = time.strptime(message_date.split('(')[0].replace(",",""), time_format)
                except ValueError as e:
                    try:
                        time_format = "%a %d %b %Y %H:%M:%S"
                        tsp = time.strptime(message_date.split('(')[0][:-5].replace(",",""), time_format)
                    except ValueError as e:
                        try:
                            time_format = "%a %d %b %Y %H:%M "
                            tsp = time.strptime(message_date.split('(')[0][:-5].replace(",",""), time_format)
                        except ValueError as e:
                            try:
                                time_format = "%a %b %d %H:%M:%S %Y"
                                tsp = time.strptime(message_date.split('(')[0][:-5].replace(",",""), time_format)
                            except BaseException as err:
                                try:
                                    ttt = pd.Timestamp(message_date)
                                    return ttt
                                except BaseException as err:
                                    print(err, "Return timestamp as 0")
                                    #flag = True
                                    return 0
    time_format = "%Y-%m-%d %H:%M:%S"
    return(pd.Timestamp(time.strftime(time_format, tsp)))

def get_status_page(document):
    #html = get_url_page(pull_url)
    try:
        #document = PyQuery(html)
        # 获取该PR状态
        status = document('#partial-discussion-header .State').attr('title').split(' ')[1]
        return status
    except BaseException as e:
        print("No status!")
        return None

# 获取html页面文本
def get_url_page(url):
    response = requests.get(url)
    logging.info(response.status_code)

    if response.status_code == 200:
        return response.text
    else:
        if response.status_code == 429:
            print("Wait 3min "+url)
            time.sleep(180)
            response = requests.get(url)
            logging.info(response.status_code)
            if response.status_code == 200:
                return response.text
            elif response.status_code == 429:
                print("Wait 30min "+url)
                time.sleep(1800)
                response = requests.get(url)
                logging.info(response.status_code)
            else:
                #print(response.text) 
                raise Exception("get url page error: responce status code" + str(response.status_code))
        else:
            raise Exception("get url page error: responce status code" + str(response.status_code))



# 解析issue页，爬取页面的issue列表
def parse_issue_page(html, url, status, proj):
    #print("Parsing issue pages")

    # 用于存储所有issue信息
    issue_list_per_page = list()

    # 实例化PyQuery对象
    document = PyQuery(html)

    # 过滤获取issue所有标签
    issue_all = document('div').filter('.js-issue-row')

    # items() 转化为list
    issue_items = issue_all.items()

    # 获取每一个issue
    for item in issue_items:
        head = item('a').filter('.h4')
        # issue信息项
        id     = re.findall(r"\d+", item.attr('id'))[0]

        title  = head.text()
        category   = item('.IssueLabel').text()
        link   = urljoin(url, head.attr('href'))
        if id!=link.split('/')[-1]:
            print("Wrong ID!")
        # time   = item('span').filter('.opened-by').children('relative-time').attr('datetime')
        author = item('.opened-by a').text()
        # 没有添加标签的类别，使用default代替
        if category == '':
            category = 'default'

        #判断是issue还是pull,得到repo实际名称
        i = 0
        path = ""
        while path!="issues" and path!="pulls":
            i+= 1
            path = urlparse(url).path.split('/')[i]
        path = urlparse(url).path.split('/')[i-1]
        issue_type = urlparse(url).path.split('/')[i]
        #path = urlparse(url).path.split('/')[1].lower()  #called source from now on
        #print(issue_type)
        # 使用json类型保存
        item_info = {
            'id'      :  proj + '#' + path + "#" + id,
            'source'  :  proj,
            'title'   :  title.replace("'", "''"),
            'category':  category,
            'type'    :  issue_type,
            'link'    :  link,
            'answered':  'no',
            'status'  :  status,
            'author'  :  author,
            'content' :  ''
        }
        if issue_type!="issues":
            item_info['id'] = proj + '#' + path + "#" + issue_type + id
        #print(item_info['id'])
        #Low efficiency, moved to get_issue_comments
        #issue_status = get_status_page(link)
        #if issue_status != None :
        #    item_info['status'] = issue_status
        
        issue_list_per_page.append(item_info)
        #print(item_info['id'])

    logging.info(issue_list_per_page)

    # 下一页链接
    next_page_link = document('.next_page').attr('href')
    logging.info(next_page_link)

    # 判断下一页链接是否disabled
    if next_page_link != None:
        next_page_link = urljoin(url, next_page_link)

    return issue_list_per_page, next_page_link


# 循环获取所有issue页面获取所有issue项, 返回issue list
def get_issues(url):
    try:
        df = pd.read_csv("/mnt/data0/proj_osgeo/gitrepository.csv")
        #print(df.head(5))
        proj = str(df.loc[df["Repos"] == url.replace('/issues', '')]["Projects"].values[0]).lower() # Find the project from the repo
    except BaseException as err:
        #print("Haven't Mined!")
        df = pd.read_csv("/mnt/data0/proj_osgeo/gitrepository_sub.csv")
        #print(df.head(5))
        proj = str(df.loc[df["Repos"] == url.replace('/issues', '')]["Projects"].values[0]).lower() # Find the project from the repo
    
    html = get_url_page(url)
    issue_list, next_page = parse_issue_page(html, url, 'opened', proj)

    while next_page != None:
        html = get_url_page(next_page)
        issue_list_per_page, next_page = parse_issue_page(html, url, 'opened', proj)
        issue_list += issue_list_per_page

    document = PyQuery(html)
    closed_url = urljoin(url, document('#js-issues-toolbar .table-list-header-toggle.states a').not_('.selected').attr('href'))

    # 爬取已关闭的issue
    html = get_url_page(closed_url)
    temp_list, next_page = parse_issue_page(html, closed_url, 'closed', proj)

    issue_list += temp_list

    while next_page != None:
        html = get_url_page(next_page)

        issue_list_per_page, next_page = parse_issue_page(html, closed_url, 'closed', proj)
        issue_list += issue_list_per_page


    logging.info(issue_list)
    return issue_list

def get_pulls(url):
    try:
        df = pd.read_csv("/mnt/data0/proj_osgeo/gitrepository.csv")
        #print(df.head(5))
        proj = str(df.loc[df["Repos"] == url.replace('/pulls', '')]["Projects"].values[0]).lower() # Find the project from the repo
    except BaseException as err:
        #print("Haven't Mined!")
        df = pd.read_csv("/mnt/data0/proj_osgeo/gitrepository_sub.csv")
        proj = str(df.loc[df["Repos"] == url.replace('/pulls', '')]["Projects"].values[0]).lower() # Find the project from the repo

    
    html = get_url_page(url)
    pull_list, next_page = parse_issue_page(html, url, 'opened', proj)

    while next_page != None:
        html = get_url_page(next_page)
        pull_list_per_page, next_page = parse_issue_page(html, url, 'opened', proj)
        pull_list += pull_list_per_page

    document = PyQuery(html)
    closed_url = urljoin(url, document('#js-issues-toolbar .table-list-header-toggle.states a').not_('.selected').attr('href')).replace('/issues?', '/pulls?')

    # 爬取已关闭的pull
    html = get_url_page(closed_url)
    temp_list, next_page = parse_issue_page(html, closed_url, 'closed', proj)

    pull_list += temp_list

    while next_page != None:
        html = get_url_page(next_page)
        pull_list_per_page, next_page = parse_issue_page(html, closed_url, 'closed', proj)
        pull_list += pull_list_per_page


    logging.info(pull_list)
    return pull_list


def get_pull_detail(pull_url):
    answered = 'no'
    html = get_url_page(pull_url)

    document = PyQuery(html)
    # 获取该PR状态
    try:
        status = document('#partial-discussion-header .State').attr('title').split(' ')[1]
    except BaseException as e:
        print("No status")

    # 获取PR提交者
    pull_author = document('.TableObject-item.TableObject-item--primary .author.text-bold.link-gray').text()

    timeline = list()
    comments = document('.timeline-comment').items()


    for comment in comments:
        author = comment('a').filter('.author').text()
        header = comment('.timeline-comment-header').text().replace("'", "''")
        timestamp = comment('.timeline-comment-header h3 a').children('relative-time').attr('datetime')
        comment_text = comment('table').text()

        comment_item = {
            'author'    : author,
            'header'    : header,
            'timestamp' : time_parse(timestamp),
            'comment'   : comment_text
        }

        timeline.append(comment_item)

        if author != pull_author:
            answered = 'yes'

    closed_time = str()
    if 'Merged' == status:
        closed_time = document('#partial-discussion-header .TableObject-item--primary relative-time').attr('datetime')
    elif 'Closed' == status:
        closed_time = document('.discussion-item-closed relative-time').attr('datetime')

    comment_item = {
        'author': pull_author,
        'header': "Record Merged or Closed time",
        'timestamp': time_parse(closed_time),
    }
    timeline.append(comment_item)

    logging.info(timeline)
    return timeline, answered, status


def get_issue_comments(issue_url, issue_id):
    answered = 'no'
    html = get_url_page(issue_url)
    document = PyQuery(html)
    issue_author = document('.TableObject-item.TableObject-item--primary .author.text-bold.link-gray').text()
    timeline = list()
    comments = document('.timeline-comment').items()

    issue_status = get_status_page(document)
    
    for comment in comments:


        author = comment('a').filter('.author').text()
        header = comment('.timeline-comment-header').text().replace("'", "''")
        timestamp = comment('.timeline-comment-header h3 a').children('relative-time').attr('datetime')
        comment_text = comment('table').text().replace("'", "''")


        comment_item = {
            'issue_id'  : issue_id,
            'author'    : author,
            'header'    : header,
            'timestamp' : time_parse(timestamp),
            'comment'   : comment_text
        }
        #print(comment_item)
        timeline.append(comment_item)

        if author != issue_author:
            answered = 'yes'
    #return comments
    return timeline, answered, issue_status

def get_all_issues_comments(issue_list):
    issue_db = Issue()
    for issue in issue_list:
        #print(issue)
        timeline, answered, issue_status = get_issue_comments(issue['link'], issue['id']) #For temporary update threads
        opened_time = timeline[0]['timestamp']
        latest_time = timeline[-1]['timestamp']
        issue['opened_time'] = opened_time
        issue['latest_time'] = latest_time
        issue['comment_number'] = len(timeline) - 1
        issue['answered'] = answered
        # issue['content'] = pickle.dumps(timeline)
        # print(issue['content'])
        # print(issue)
        
        if issue_status != None :
            issue['status'] = issue_status
        #print(issue['status'])
        
        issue_db.save_one_thread(issue)
        issue_db.save_all_comments(timeline) #For temporary update threads
    logging.info(issue_list)
    return issue_list