import psycopg2 as ps
import psycopg2.extras as e
import os
con = ps.connect("dbname='seeDB' user='postgres' host='localhost' password='fdfsgdcaf44'")

def query1():
    """
    Return the result of the query as a list of lists
    :return:
    """

    cur = con.cursor()
    cur.execute("select * from census limit 5")
    rows = cur.fetchall()
    print(rows)


def query2():
    cur = con.cursor(cursor_factory=e.DictCursor)
    cur.execute("select * from census limit 5")
    rows = cur.fetchall()
    print(rows[0])


def query3():
    filename= os.path.join(os.path.dirname(os.getcwd()),"Data/testfile.csv")
    fhandle=open(filename,'w')
    cur = con.cursor()
    cur.copy_to(fhandle,'census',sep=",")

if __name__=="__main__":
    query1()

