import mysql.connector
import time
import subprocess
import config
import random

codeConfig = {
    'probPath': 'E:/SmartJudger/data/prob/',
    'execPath': 'E:/SmartJudgerWatcher/data/',
    'codePath': 'E:/SmartJudgerWatcher/data/'
}

langCompileConfig = {
    0: "g++ -x c++ %(src)s -o %(target)s",
    1: "",
    2: "",
    3: "",
    4: "",
    5: "",
    6: "",
    7: "",
    8: "",
    9: "",
    10: "",
    11: ""
}

langRunConfig = {
    0: "%(target)s",
    1: "",
    2: "",
    3: "",
    4: "",
    5: "",
    6: "",
    7: "",
    8: "",
    9: "",
    10: "",
    11: ""
}

def compile(src, lang, target):
    cmd = langCompileConfig[lang] % {'src': src, 'target': target}
    print(cmd)
    p = subprocess.Popen(cmd,
    shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()
    return (retval, p.stdout.read())

def runprog(src, lang, target):
    return 0

def query(db, sql, cmd, dat):
    sql.execute(cmd, dat)
    return sql

def freeQuery(db):
    db.commit()

def saveCode(sid, pid, lang, code, path):
    fp = open(path , 'w')
    fp.write(code)
    fp.close()

def doTask(db, sql, sid, pid, lang):
    # set status
    # result = query("".join(["UPDATE status SET status = 1 WHERE sid = ", str(sid)]))
    # debug
    print("Waiting: sid:%s pid:%s lang:%s" % (sid, pid, lang))
    sql = query(db, sql, "UPDATE status SET status=1 WHERE sid=%s" % (sid), ())
    # get code
    print("Getting code: sid:%s pid:%s lang:%s" % (sid, pid, lang))
    sql = query(db, sql, "SELECT sid, code FROM statuscode WHERE sid = %s" % (sid), ())
    src_path = "%s/%s_%d.code" % (codeConfig["codePath"], sid, random.randint(0, 65536))
    out_path = "%s/%s_%d.exe" % (codeConfig["execPath"], sid, random.randint(0, 65536))
    for (sid, code) in sql:
        saveCode(sid, pid, lang, code, src_path)
    out_path = "%s/%s_%d.code" % (codeConfig["codePath"], sid, random.randint(0, 65536))
    freeQuery(db)
    print("Compile: sid:%s pid:%s lang:%s" % (sid, pid, lang))
    (ret_code, ret_dat) = compile(src_path, lang, out_path)
    if(ret_code != 0):
        sql = query(db, sql, "UPDATE status SET status=7 WHERE sid=%s" % (sid), ())
        sql = query(db, sql, "UPDATE statuscode SET ret = %s WHERE sid = %s", (bytes.decode(ret_dat), sid))
    else:
        sql = query(db, sql, "UPDATE status SET status=8 WHERE sid=%s" % (sid), ())

def __main__():
    db = mysql.connector.connect(**config.dbconfig.sqlconfig)
    while True:
        print( "Excuting...")
        sql = db.cursor()
        sql = query(db, sql, "SELECT sid, pid, lang, status FROM status WHERE status = 0 LIMIT 0, 1 ", ())
        for (sid, pid, lang, status) in sql:
            doTask(db, sql, sid, pid, lang)
        sql.close()
        db.commit()
        time.sleep(2)

    db.close()

__main__()
