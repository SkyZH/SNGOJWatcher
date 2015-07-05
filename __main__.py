import mysql.connector
import time
import subprocess



codeConfig = {
    'basePath': 'E:/SmartJudger/data/prob/',
    'targetPath': 'E:/SmartJudgerWatcher/data/'
}



def compile(path, pid, lang, status):
    cmd = "".join([
        'g++ -x c++ ',
        codeConfig['basePath'],
        path,
        ' -o ',
        codeConfig['targetPath'],
        path,
        '.exe'
    ])
    print(cmd)
    p = subprocess.Popen(cmd,
    shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
        print (line)
    retval = p.wait()
    return retval

def __main__():
    sql = mysql.connector.connect(**sqlconfig)
    while True:
        time.sleep(2);

        cursor = sql.cursor()

        query = ("SELECT jid, path, pid, lang, status FROM judge WHERE status = 0 LIMIT 0, 1 ")
        cursor.execute(query)
        for (jid, path, pid, lang, status) in cursor:
            query = ("".join(["UPDATE judge SET status = 1 WHERE jid = ", str(jid)]))
            cursor.execute(query)
            print("Running {0}".format(path))
            __status = compile(path, pid, lang, status);
            if(__status != 0):
                query = ("".join(["UPDATE judge SET status = 7 WHERE jid = ", str(jid)]))
            else:
                query = ("".join(["UPDATE judge SET status = 8 WHERE jid = ", str(jid)]))
            cursor.execute(query)
        cursor.close()


    sql.close()

__main__()
