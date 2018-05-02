import psycopg2 as ps
import psycopg2.extras as e
import os
import csv
con = ps.connect("dbname='seeDB' user='postgres' host='localhost' password='<insert_your_pwd>'")
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


def query2():
    cur = con.cursor(cursor_factory=e.DictCursor)
    cur.execute("select * from census limit 5")
    rows = cur.fetchall()
    print(rows[0])


def query3(view):
    filename= os.path.join(os.path.dirname(os.getcwd()),"Data/{}.csv".format(view))
    fhandle=open(filename,'w')
    cur = con.cursor()
    cur.execute("select * from {} limit 5".format(view))
    rows = cur.fetchall()
    cur.copy_to(fhandle,rows,sep=",")

if __name__=="__main__":
    target_view="married"
    reference_view="unmarried"
    # Uncomment these to create tables/views and load data

    # create_tables()
    # insert_data()
    # create_views()

