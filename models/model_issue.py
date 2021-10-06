from config.database import HOST, PORT, USER, PASSWORD, DATABASE
import psycopg2
import datetime
import logging
from pyquery import PyQuery
import pandas as pd

utc_format = '%Y-%m-%dT%H:%M:%SZ'

class Issue(object):

    # 析构函数，断开数据库连接
    def __del__(self):
        self.cursor.close()
        self.db.close()


    # 构造函数，初始化并连接数据库
    def __init__(self, Flevel=logging.DEBUG):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
        fh = logging.FileHandler(__name__)
        fh.setFormatter(fmt)
        fh.setLevel(Flevel)
        self.logger.addHandler(fh)
        try:
            self.db = psycopg2.connect(
                host=HOST,
                port=PORT,
                user=USER,
                password=PASSWORD,
                database=DATABASE)
                #charset='utf8')
            self.cursor = self.db.cursor()
        except Exception as e:
            logging.error("Database connect error:%s" % e)


    # 批量保存issues
    def save_all(self, issue_list):

        for issue in issue_list:
            sql = """INSERT INTO issue(title, source, type, No, opened_time, latest_time, comment_number, answered, status, author, link, content)
                             values ('%s', '%s', '%s','%s', '%s', '%s', '%s', '%s','%s', '%s', '%s')""" % (
            issue['title'], issue['source'], issue['type'], issue['id'], issue['opened_time'], issue['latest_time'], issue['comment_number'], issue['answered'], issue['status'], issue['link'], issue['content'])

            try:
                self.cursor.execute(sql)
                self.db.commit()
            except:
                try:  # 插入失败表示数据库存在次issue，转为update更新
                    sql_update = """UPDATE issue SET title='%s', type='%s', opened_time='%s', latest_time='%s', comment_number='%s', answered='%s', status='%s', content='%s' WHERE No='%s'""" % (
                    issue['title'], issue['type'], issue['opened_time'], issue['latest_time'], issue['comment_number'], issue['answered'], issue['status'], issue['content'], issue['id'])
                    self.cursor.execute(sql_update)
                    self.db.commit()
                    #print("更新完成")
                except Exception as e:
                    logging.error("update error:%s" % e)
                    self.db.rollback()


    # 保存一条issue记录
    def save_one(self, issue):
        sql = """INSERT INTO issue(title, source, type, No, opened_time, latest_time, comment_number, answered, status, author, link, content)
                                         values('%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
            issue['title'], issue['source'], issue['type'], issue['id'], issue['opened_time'], issue['latest_time'], issue['comment_number'], issue['answered'], issue['status'], issue['author'], issue['link'], issue['content'])

        try:
            self.cursor.execute(sql)
            self.db.commit()
            #print("%s : Insert successfully" % issue['id'])
        except Exception as e:
            try:  # 插入失败表示数据库存在次issue，转为update更新
                #print("Exception: %s" % e)
                sql_update = """UPDATE issue SET title='%s', type='%s', opened_time='%s', latest_time='%s', comment_number='%s', answered='%s', status='%s', content='%s' WHERE No='%s'""" % (
                    issue['title'], issue['type'], issue['opened_time'], issue['latest_time'], issue['comment_number'], issue['answered'], issue['status'], issue['content'], issue['id'])
                self.cursor.execute(sql_update)
                self.db.commit()
                #print("%s : Update successfully" % issue['id'])
            except Exception as err:
                print("Exception: %s" % err)
                logging.error("update error:%s" % err)
                self.db.rollback()

    def save_one_thread(self, issue):
        sql = """INSERT INTO thread(thread_id, thread_name, project_id, thread_type, thread_status)
                                         values('%s', '%s', '%s', '%s', '%s')""" % (
            issue['id'], issue['title'], issue['source'], issue['type'], issue['status'])

        try:
            self.db.commit()
            self.cursor.execute(sql)
            self.db.commit()
            #print("%s : Insert successfully" % issue['id'])
        except Exception as e:
            try:  # 插入失败表示数据库存在此issue，转为update更新
                self.db.commit()
                #print("Exception: %s" % e)
                sql_update = """UPDATE thread SET thread_name='%s', project_id='%s', thread_type='%s', thread_status='%s' WHERE thread_id='%s'""" % (
                    issue['title'], issue['source'], issue['type'], issue['status'], issue['id'])
                self.cursor.execute(sql_update)
                self.db.commit()
                #print("%s : Update successfully" % issue['id'])
            except Exception as err:
                print("Exception: %s" % err)
                logging.error("update error:%s" % err)
                self.db.rollback()

    #寻找github账户的邮箱
    def save_aliase(self, github_id):
        github_mail = ""
        source = "Github"
        
        #try:
        #    document = PyQuery("https://github.com/"+github_id)

        #except BaseException as err:
        #    print(err)
    
        sql_aliase = """INSERT INTO aliase(aliase_id, mailaddress, source)
                                        values('%s', '%s', '%s')""" % (
        github_id, github_mail, source)

        try:
            self.db.commit()
            self.cursor.execute(sql_aliase)
            self.db.commit()
        except Exception as err:
            sql_aliase = """UPDATE aliase SET mailaddress ='%s', source='%s' WHERE aliase_id='%s'"""% (
                            github_mail, source, github_id)
                
            self.db.commit()
            self.cursor.execute(sql_aliase)
            self.db.commit()
            #print(err)

        return github_id

            


    def save_all_comments(self, timeline):
        counter = 0
        for comment in timeline:
            if comment['comment'] == '' or comment['timestamp'] == 'None':
                continue
            counter += 1

            self.save_aliase(comment['author'])
            #Timestamp 格式转换
            try:
                comment['timestamp'] = pd.Timestamp(comment['timestamp'])
            except BaseException as e:
                print(e)

            sql = """INSERT INTO comment(comment_id, thread_id, author_aliase_id, comment_text, timestamp)
                                         values('%s', '%s', '%s', '%s','%s')""" % (
            comment['issue_id']+'#'+str(counter), comment['issue_id'], comment['author'], comment['comment'], comment['timestamp'])
            
            try:
                self.db.commit()
                self.cursor.execute(sql)
                self.db.commit()
                #print("插入成功")
            except:
                try:  # 插入失败表示数据库存在comment，转为update更新
                    self.db.commit()
                    sql_update = """UPDATE comment SET thread_id='%s', author_aliase_id='%s', comment_text='%s', timestamp='%s' WHERE comment_id='%s'""" % (
                    comment['issue_id'], comment['author'], comment['comment'], comment['timestamp'], comment['issue_id']+'#'+str(counter))
                    self.cursor.execute(sql_update)
                    self.db.commit()
                    #print("更新完成", comment['issue_id']+'#'+str(counter) )
                except Exception as err:
                    print("Exception: %s" % err, comment['issue_id']+'#'+str(counter))
                    logging.error("update error:%s" % err)
                    self.db.rollback()


    # 获取对应源的issue信息
    def issue_info(self, source):
        total_count = self.get_issue_count(source)
        closed_count = self.get_issue_count(source, "status='closed'")
        select_results = self.select('issue', 'opened_time, latest_time, No', source, "status='closed'")
        highest_attention = self.select('issue', 'No, comment_number', source, "comment_number in (select max(comment_number) from tars.issue where source=%s)" % source)

        # 获取关注度最高的评论
        if highest_attention:
            comment_number = highest_attention[0][1]
        else:
            comment_number = 0

        # 计算平均关闭时间，并获取最长处理时间及对应的issue
        avg_time = datetime.timedelta()
        longest_handling_time = datetime.timedelta(0)
        issue_with_longest_handling_time = "None"

        for item in select_results:
            opened_time = datetime.datetime.strptime(item[0], utc_format)
            closed_time = datetime.datetime.strptime(item[1], utc_format)
            temp_delta = (closed_time - opened_time)

            if temp_delta > longest_handling_time:
                longest_handling_time = temp_delta
                issue_with_longest_handling_time = item[2]
            avg_time += temp_delta

        closed_len = len(select_results)

        if 0 == closed_len:
            avg_time = 0
        else:
            avg_time /= closed_len

        most_attention_issue = "None"

        # 获取关注度最高的comment
        if highest_attention:
            most_attention_issue = ""
            for item in highest_attention:
                most_attention_issue += " "
                most_attention_issue += item[0]

        # 打印issue统计信息
        return '''Issues Info of %s :
        total count: %s
        closed count: %s
        average opening time: %s
        highest attention: %s
        max number of comment: %s
        issue with longest handling time: %s
        longest handling time: %s
        ''' % (source, total_count, closed_count, avg_time, most_attention_issue, comment_number, issue_with_longest_handling_time, longest_handling_time)


    # 获取issue总数
    def get_issue_count(self, source, condition=""):
        return self.count(table='issue', source=source, condition=condition)


    # select语句封装
    def select(self, table, column, source, condition):
        if condition == "":
            sql = "SELECT %s FROM %s WHERE source=%s" % (column, table, source)
        else:
            sql = "SELECT %s FROM %s WHERE source=%s and %s" % (column, table, source, condition)

        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            print("Error occur: %s" % e)


    # 计数函数封装
    def count(self, table, source, condition=""):
        if condition == "":
            sql = "SELECT count(1) FROM %s WHERE source=%s" % (table, source)
        else:
            sql = "SELECT count(1) FROM %s WHERE source=%s and %s" % (table, source, condition)
        res = 0

        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            res = result[0][0]
        except Exception as e:
            print("Error: fetch data error with message: %s" % e)

        return res