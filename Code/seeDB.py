import psycopg2 as ps
import psycopg2.extras as e
import os
con = ps.connect("dbname='seeDB' user='postgres' host='localhost' password='Zero.kira1412'")


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

