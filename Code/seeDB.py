import psycopg2 as ps
import psycopg2.extras as e
import os
import csv
import numpy as np
from sklearn.preprocessing import normalize
import scipy as sc
import itertools
import time
import collections

con = ps.connect("dbname='seeDB' user='postgres' host='localhost' password='fsfdgfad' ")
DATA_PATH=os.path.join(os.path.dirname(os.getcwd()),"Data/splits/")

# 0 - age : continuous.
# 1 - workclass : Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked.
# 2 - fnlwgt : continuous.
# 3 - education : Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool.
# 4 - education-num : continuous.
# 5 - marital-status : Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse.
# 6 - occupation : Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces.
# 7 - relationship : Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried.
# 8 - race : White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black.
# 9 - sex : Female, Male.
# 10 - capital-gain : continuous.
# 11 - capital-loss : continuous.
# 12 - hours-per-week : continuous.
# 13 - native-country : United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands.
# 14 - economic_indicator : >50K, <=50K

function_list=["avg", "sum", "min", "max", "count"]
dimensions=["workclass" , "education" , "occupation" , "relationship" , "race" , "sex" , "native_country","economic_indicator" ]
measures=["age","fnlwgt","hours-per-week","capital-gain","capital-loss"]


def normalize(rows):
    """
    Function to normalize the f(m) column
    :param rows:
    :return:
    """
    x = normalize(rows[:, [1]], axis=0)
    return np.concatenate((rows[:, [0]], x), axis=1)

def compute_KL(p,q):
    """
    Function to compute the KL divergence between two arrays
    :param p: First PDF
    :param q: Second PDF
    :return:
    """
    # This module automatically normalizes the arrays sent to it
    # This will be our utility measure. We need not be concerned with equal lengths for p and q
    # since in our specification we are grouping on married and unmarried which will have same number of groups
    return sc.stats.entropy(p,q)

def uneven_kl_divergence(pk,qk):
    """
    In the event that the arrays aren't of same length
    :param pk:
    :param qk:
    :return:
    """
    if len(pk)>len(qk):
        pk = np.random.choice(pk,len(qk))
    elif len(qk)>len(pk):
        qk = np.random.choice(qk,len(pk))
    return np.sum(pk * np.log(pk/qk))


def create_tables():
    """
    Create 10 tables to load the data splits
    :return:
    """
    cur = con.cursor()
    for i in range(10):
        command="create table census_{} (age real, workclass text, fnlwgt real, education text, education_num real, marital_status text, occupation text, relationship text, race text, sex text, capital_gain real, capital_loss real, hours_per_week real, native_country text, economic_indicator text)".format(i+1)
        cur.execute(command)
    cur.close()
    # commit the changes
    con.commit()

def insert_data():
    """
    Insert values into those tables
    :return:
    """
    cur = con.cursor()
    for i in range(1,11):
        with open(os.path.join(DATA_PATH,"test_{}.csv".format(i)), 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO census_{} VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s)".format(i),row)

    cur.close()
    # commit the changes
    con.commit()


def create_views():
    """
    Create 10 tables to load the data splits
    :return:
    """
    cur = con.cursor()
    for i in range(10):
        command=""" create view married_{} as select * from census_{} where marital_status in (' Married-AF-spouse', ' Married-civ-spouse', ' Married-spouse-absent',' Separated');
                    create view unmarried_{} as select * from census_{} where marital_status in (' Never-married', ' Widowed',' Divorced');""".format(i+1,i+1,i+1,i+1)
        cur.execute(command)
    cur.close()
    # commit the changes
    con.commit()


def query_tuples(command):
    cur = con.cursor()
    cur.execute(command)
    rows = cur.fetchall()
    return np.asarray(rows)


def query3(view):
    filename= os.path.join(os.path.dirname(os.getcwd()),"Data/{}.csv".format(view))
    fhandle=open(filename,'w')
    cur = con.cursor()
    cur.execute("select * from {} limit 5".format(view))
    rows = cur.fetchall()
    cur.copy_to(fhandle,rows,sep=",")

def query_timer(command):
    t0 = time.time()
    cur = con.cursor(cursor_factory=e.DictCursor)
    cur.execute(command)
    t1 = time.time()
    print('Time taken by this query: %0.3f' % (t1 - t0))

class SeeDB(object):
    def __init__(self):
        self.seen=collections.defaultdict(dict)
        self.prune={}
        self.phase_count=1

    def share_opt_phase(self):
        """
        Generate different combinations of function, dimension attribute and measurement attribute
        and get the utilities in a particular phase or partition
        :return:
        """
        for (f,a,m) in itertools.product(function_list,dimensions,measures):
            if (f,a,m) not in self.prune:
                # Query on target db
                command_target="select {},{}({}) from married_{} group  by {}".format(a,f,m,self.phase_count,a)
                # Query on the refernce db
                command_reference="select {},{}({}) from unmarried_{} group  by {}".format(a,f,m,self.phase_count,a)
                target_tuples=query_tuples(command_target)
                reference_tuples=query_tuples(command_reference)
                if len(target_tuples) == len(reference_tuples):
                    self.seen[(f,a,m)]=compute_KL(target_tuples,reference_tuples)
                else:
                    pk=0
                    qk=0
                    uneven_kl_divergence(target_tuples,reference_tuples)


        self.phase_count+=1

    def prune_opt(self):
        """
        Prune specific triples based on KL divergence(utility)
        :return:
        """
        pass








if __name__=="__main__":

    # Uncomment these to create tables/views and load data

    # create_tables()
    # insert_data()
    # create_views()

    s=SeeDB()
    s.share_opt_phase()






