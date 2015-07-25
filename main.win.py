import mysql.connector
import time
import subprocess
import config
import random
import os
import traceback

codeConfig = {
    'probPath': './data/prob/',
    'execPath': './data/exec/',
    'codePath': './data/code/'
}

langCompileConfig = {
    0: "g++ -x c++ -O2 -Wall -lm -DONLINE_JUDGE --static --std=c++98 -fno-asm %(src)s -o %(target)s",
    1: "gcc -x c -O2 -Wall -lm -DONLINE_JUDGE --static --std=c99 -fno-asm %(src)s -o %(target)s",
    2: "g++ -x c++ -O2 -Wall -lm -DONLINE_JUDGE --static --std=c++11 -fno-asm %(src)s -o %(target)s",
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
    1: "%(target)s",
    2: "%(target)s",
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
    p = subprocess.Popen(cmd,
    shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()
    return (retval, p.stdout.read())

def query(db, sql, cmd, dat):
    sql.execute(cmd, dat)
    return sql

def freeQuery(db):
    db.commit()

def saveCode(sid, pid, lang, code, path):
    fp = open(path , 'w')
    fp.write(code)
    fp.close()

def readData(datapath):
    fp = open(datapath , 'r')
    __dat = fp.read()
    fp.close()
    return __dat

def judge_result(user_result, currect_result):
    curr = currect_result.replace('\r','').rstrip()
    user = user_result.replace('\r','').rstrip()
    if curr == user:
        return 2
    if curr.split() == user.split():
        return 10
    return 3

def doData(runpath, datapath):
    p = subprocess.Popen(runpath,
    shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    ret_code = 9
    try:
        out, err = p.communicate(input = str.encode(readData("%s.in" % (datapath))), timeout = 1)
    except subprocess.TimeoutExpired:
        p.kill()
        ret_code = 5
    if(ret_code == 9):
        if(p.returncode != 0):
            ret_code = 6
        else:
            ret_code = judge_result(bytes.decode(out), readData("%s.ans" % (datapath)))
    return (ret_code, b"")

def doRun(pid, lang, srcpath, outpath):
    cmd = langRunConfig[lang] % {'src': srcpath, 'target': outpath}
    prob_path = "%s/%s/" % (codeConfig["probPath"], pid)

    ret_code = 2
    __dat = ""
    for parent, dirnames, filenames in os.walk(prob_path):
        for filename in filenames:
            if os.path.splitext(filename)[1] == '.in':
                data_path = os.path.join(parent, os.path.splitext(filename)[0])
                ret_val, ret_data = doData(cmd, data_path)
                if(ret_val != 2):
                    ret_code = ret_val
                    err_dat = os.path.splitext(filename)[0]
                    __dat = __dat.join("Error on Data %s \n" % err_dat)
                    break
    return (ret_code, __dat)

def doTask(db, sql, sid, pid, lang):
    # set status
    # debug
    print("Waiting: sid:%s pid:%s lang:%s" % (sid, pid, lang))
    sql = query(db, sql, "UPDATE status SET status=1 WHERE sid=%s" % (sid), ())
    # get code
    print("Getting code: sid:%s pid:%s lang:%s" % (sid, pid, lang))
    rows = ()
    for i in range(5):
        sql = query(db, sql, "SELECT sid, code FROM statuscode WHERE sid = %s" % (sid), ())
        rows = sql.fetchall()
        freeQuery(db)
        if(len(rows) > 0):
            break
        time.sleep(1)
    src_path = "%s/%s_%d.code" % (codeConfig["codePath"], sid, random.randint(0, 65536))
    out_path = "%s/%s_%d.exe" % (codeConfig["execPath"], sid, random.randint(0, 65536))

    for (sid, code) in rows:
        saveCode(sid, pid, lang, code, src_path)
    freeQuery(db)

    print("Compile: sid:%s pid:%s lang:%s" % (sid, pid, lang))
    (ret_code, ret_dat) = compile(src_path, lang, out_path)
    if(ret_code != 0):
        sql = query(db, sql, "UPDATE status SET status=7 WHERE sid=%s" % (sid), ())
        sql = query(db, sql, "UPDATE statuscode SET ret = %s WHERE sid = %s", (bytes.decode(ret_dat), sid))
    else:
        sql = query(db, sql, "UPDATE status SET status=8 WHERE sid=%s" % (sid), ())
        freeQuery(db)
        print("Running: sid:%s pid:%s lang:%s" % (sid, pid, lang))
        ret_stat, ret_data = doRun(pid, lang, src_path, out_path)
        sql = query(db, sql, "UPDATE status SET status=%d WHERE sid=%s" % (ret_stat, sid), ())
        sql = query(db, sql, "UPDATE statuscode SET ret = %s WHERE sid = %s", (ret_data, sid))
        freeQuery(db)
        if(ret_stat == 2):
            sql = query(db, sql, "UPDATE problemset SET accepted = accepted + 1 WHERE pid = %s" % (pid),())

def __main__():
    db = mysql.connector.connect(**config.dbconfig.sqlconfig)
    while True:
        sql = db.cursor()
        sql = query(db, sql, "SELECT sid, pid, lang, status FROM status WHERE status = 0 LIMIT 0, 1 ", ())
        for (sid, pid, lang, status) in sql:
            doTask(db, sql, sid, pid, lang)
            print("Task End")
        sql.close()
        db.commit()
        time.sleep(1)

    db.close()

__main__()
