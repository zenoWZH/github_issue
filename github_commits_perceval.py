import json
import sys
import modin.pandas as modinpd
import pandas as pd
from perceval.backends.core.git import Git
from models.model_Commits import Commits

from sqlalchemy import create_engine
from sqlalchemy import types as sqltype
from config.database import HOST, PORT, USER, PASSWORD, DATABASE

import psycopg2

def add_aliase(aliase_id, mailaddress, source):

    try:
        db = psycopg2.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE)
            #charset='utf8')
        cursor = db.cursor()
    except Exception as e:
        logging.error("Database connect error:%s" % e)

    #aliase_id, mailaddress = aliase.replace('<', ' ').replace('>', ' ').split()
    sql_aliase = """INSERT INTO aliase(aliase_id, mailaddress, source)
                                        values('%s', '%s', '%s')""" % (aliase_id, mailaddress, source)
    try:
        db.commit()
        cursor.execute(sql_aliase)
        db.commit()
    except Exception as err:
        sql_aliase = """UPDATE aliase SET mailaddress='%s', source='%s' WHERE aliase_id='%s' """ % (mailaddress, source, aliase_id)
        #print(err)
        pass
    return aliase_id

df = pd.read_csv("../gitrepository.csv")
github_source_url = df["Repos"].values

# url for the git repo to analyze
for url in github_source_url:
    repo_url = url
    # directory for letting Perceval clone the git repo
    repo_dir = '/tmp/temp.git'
    counter = 0
    proj_name = str(df.loc[df["Repos"] == url.replace('/issues', '')]["Projects"].values[0]).lower()
    repo_name = repo_url.split('/')[-1]

    # create a Git object, pointing to repo_url, using repo_dir for cloning
    repo = Git(uri=repo_url, gitpath=repo_dir)
    repo_data = repo.fetch()
    
    #print(type(repo_data))
    # Fetch return a generator
    
    ####fw = open('./repo_fetch.json','w')
    ###fw.write('{\"all_commits\":[')
    
    all_commits = []

    # fetch all commits as an iterator, and iterate it printing each hash
    
    for commit in repo_data:
        counter += 1
        commit['data']['proj_id'] = proj_name
        commit['data']['repo'] = repo_name
        commit['data']['commit_id'] = proj_name+"#"+repo_name+"#"+str(counter)+"#"+commit['data']['commit']
        
        try:
            author= commit['data']['Author'].replace('<', ' ').replace('>', ' ').split()[0]
            author_email= commit['data']['Author'].replace('<', ' ').replace('>', ' ').split()[-1]
            commiter= commit['data']['Commit'].replace('<', ' ').replace('>', ' ').split()[0]
            commiter_email= commit['data']['Commit'].replace('<', ' ').replace('>', ' ').split()[-1]
            if author == author_email :
                author_email = ""
            if commiter == commiter_email:
                commiter_email = ""
            commit['data']['author_id'] = author
            commit['data']['author_email'] =  author_email
            commit['data']['commiter_id'] = commiter
            commit['data']['commiter_email'] = commiter_email
        except IndexError as e:
            print(commit['data']['Author'])
            print(commit['data']['Commit'])

        all_commits.append(commit['data'])


    print(counter)

    df_all_commits = pd.DataFrame(all_commits)

    df_all_commits.columns = ['commit_sha', 'commit_parents', 'commit_refs', 'author', 'author_timestamp', 'commiter',
       'commit_timestamp', 'commit_message', 'files', 'proj_id', 'repo', 'commit_id','author_aliase_id', 'author_email', 'commiter_aliase_id', 'commiter_email', 
       'Merge', 'Signed-off-by']
    
    df_all_commits['commit_timestamp'].apply(lambda x: pd.Timestamp(x))
    df_all_commits['author_timestamp'].apply(lambda x: pd.Timestamp(x))

    df_psql_commits = df_all_commits[['commit_id','proj_id','author_aliase_id', 'author_timestamp', 
    'commiter_aliase_id', 'commit_timestamp', 'commit_message', 'commit_sha', 'commit_parents', 'commit_refs']].astype(str)

    df_psql_aliases = df_all_commits[['author_aliase_id', 'author_email']].rename(\
        columns={'author_aliase_id':'aliase_id', 'author_email': 'mailaddress'}).append( \
    df_all_commits[['commiter_aliase_id', 'commiter_email']].rename( \
        columns={'commiter_aliase_id':'aliase_id', 'commiter_email': 'mailaddress'}), ignore_index=True).drop_duplicates().astype(str)
    df_psql_aliases['source'] = 'Github'

    all_filelogs = []
    file_counter = 0
    for filelog, commit_id, proj, repo in (df_all_commits[["files","commit_id","proj_id","repo"]].values):
        for log in filelog:
            file_counter+=1
            log['commit_id'] = commit_id
            log['id'] = proj+"#"+repo+"#"+str(file_counter)+"#"+log['file']
            #log['mode_before']= log['modes'][0]
            #log['mode_after']= log['modes'][-1]
        all_filelogs += filelog
        #print(filelog)
        #break
        
    df_all_filelogs = pd.DataFrame(all_filelogs)
    df_all_filelogs.head(10)

    df_all_filelogs.columns = ['modes', 'indexes', 'action', 'file_name', 'added', 'removed', 'commit_id',
       'filelog_id', 'newfile']
    
    df_psql_filelogs = df_all_filelogs[['filelog_id', 'commit_id', 'modes', 'indexes', 'action', 'file_name', 'added', 'removed', 'newfile']].astype(str)

    psql_engine = create_engine("postgresql://"+USER+":"+PASSWORD+"@"+HOST+":"+str(PORT)+"/"+DATABASE)

    
    df_psql_filelogs.to_sql(name='filelogs', con = psql_engine, if_exists= 'append', index= False)
    
    for aliase_id, mailaddress, source in df_psql_aliases.values :
        add_aliase(aliase_id, mailaddress, source)

    df_psql_commits.to_sql(name='commit', con = psql_engine, if_exists= 'append', index= False)