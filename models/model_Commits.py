from config.database import HOST, PORT, USER, PASSWORD, DATABASE
from perceval.backends.core.git import Git
import psycopg2
import datetime
import logging

class Commits(object):

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
        self.proj = ''
        self.url = ''
        self.repo = ''
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

    def add_aliase(self, aliase):

        aliase_id, mailaddress = aliase.replace('<', ' ').replace('>', ' ').split()
        sql_aliase = """INSERT INTO aliase(aliase_id, mailaddress)
                                         values('%s', '%s')""" % (aliase_id, mailaddress)
        try:
            self.db.commit()
            self.cursor.execute(sql_aliase)
            self.db.commit()
        except Exception as err:
            sql_aliase = """UPDATE aliase SET mailaddress='%s' WHERE aliase_id='%s' """ % (mailaddress, aliase_id)
            #print(err)
            pass
        return aliase_id
    
    def add_filelog(self, filelog):
        
        return

    def add_commit(self, commit):

        author = self.add_aliase(commit['Author'])
        commiter = self.add_aliase(commit['Commit'])
        
        sql = """INSERT INTO commit(commit_id, proj_id, commit_SHA, author_aliase_id, commiter_aliase_id, commit_timestamp, author_timestamp, message)
                                        values('%s', '%s', '%s', '%s', '%s','%s', '%s', '%s')""" % (
        self.proj+'#'+str(self.counter), self.proj, commit['commit'], author, commiter, commit['CommitDate'], commit['AuthorDate'], commit['message'])

        try:
            self.db.commit()
            self.cursor.execute(sql)
            self.db.commit()
            #print("插入成功")
        except BaseException as err:
            sql = """UPDATE commit SET proj_id='%s', commit_SHA='%s', author_aliase_id='%s', commiter_aliase_id='%s', commit_timestamp='%s', author_timestamp='%s', message='%s' WHERE commit_id='%s'"""% (
                    self.proj, commit['commit'], author, commiter, commit['CommitDate'], commit['AuthorDate'], commit['message'], self.proj+'#'+str(self.counter))
            print(err)
            #print("commit existed")

    def fetch(self, proj, url):

        self.repo_url = url
        # directory for letting Perceval clone the git repo
        self.repo_dir = '/tmp/temp.git'
        self.counter = 0

        self.proj = proj
 
        try:
            # create a Git object, pointing to repo_url, using repo_dir for cloning
            repo = Git(uri=self.repo_url, gitpath=self.repo_dir)
            # fetch all commits as an iterator, and iterate it printing each hash
            for commit in repo.fetch():
                self.counter += 1
                #print(commit['data'])
                self.add_commit(commit['data'])
            print(self.counter)
        except BaseException as err:
            print(err)
