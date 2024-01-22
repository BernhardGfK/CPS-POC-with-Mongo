import config
from datetime import date, timedelta
import random
import calendar

# This script generates household, weight and purchase data

# This function generates household data
def generateHouseholdData():
    """ generate household data """
    id=config.startid
    hhfile=open("hh.txt", "w")
    hhfeat=[config.bdl, config.age]
    for i in range(2, config.hhfeatnr):
        nrnv=random.randint(1, 10)
        #print("number of hhfeat values: ", nrnv)
        s=0
        w={}
        for k in range(0, nrnv):
            w[k]=random.uniform(0, 1)
            s+=w[k]
        feat={}
        for k in range(0, nrnv):
            feat[id]=w[k]/s
            id+=1
        hhfeat.append(feat)
        #print(feat)
    if id> config.firsthhid:
        print("error: id>firsthhid. There will be an overlap of ids.")
    id=config.firsthhid
    for validdate in ("2017-01-01", "2018-01-01", "2019-01-01"):
        for j in range(0, config.hhnr):
            print(id, end='\t', file=hhfile)
            print("household: ", j, id)
            id+=1
            for i in range(0, config.hhfeatnr):
                if i>=config.hhfeatstdnr and random.uniform(0, 1)<0.9:
                    print("NULL", file=hhfile, end='\t')
                    continue
                vp=random.uniform(0, 1)
                s=0
                for val in hhfeat[i]:
                    s+=hhfeat[i][val]
                    if s>=vp:
                        break
                #print("\ti: ", i, "val: ", val)
                print(val, file=hhfile, end='\t')
            print(validdate, file=hhfile)
    hhfile.close()

    # now gerenerate the sql file for sqlite to import the data
    hhsql=open("hh.sql", "w")
    print("drop table if exists households;", file=hhsql)
    print("create table households (", file=hhsql)
    print("\thousehold_id int,", file=hhsql)
    print("\tbdl_id int,", file=hhsql)
    print("\tage_id int,", file=hhsql)
    for i in range(2, config.hhfeatnr):
        if i>=config.hhfeatstdnr:
            print("\tfeat"+str(i)+"_id int default null,", file=hhsql)
        else:
            print("\tfeat"+str(i)+"_id int,", file=hhsql)
    print("\tvalid_date text);", file=hhsql)
    print(".mode tabs", file=hhsql)
    print(".import hh.txt households", file=hhsql)
    print(".headers on", file=hhsql)
    print("select * from households limit 10;", file=hhsql)
    hhsql.close()

    # generate the household axis
    hhaxfile=open("hhaxis.txt", "w")
    for b in config.bdl:
        for a in config.age:
            print(b, b, a, a, a, a, b, sep='\t', file=hhaxfile)
            print(b, b, a, a, b, b, "TOTAL", sep='\t', file=hhaxfile)
            print(b, b, a, a, "TOTAL", "TOTAL", "NULL", sep='\t', file=hhaxfile)
    hhaxfile.close()

    # generate sql file for sqlite to import the axis data
    hhaxsql=open("hhaxis.sql", "w")
    print("drop table if exists hhaxis;",file=hhaxsql)
    print("create table hhaxis (",file=hhaxsql)
    print("\tbdl_id int,",file=hhaxsql)
    print("\tbdl_name varchar(30),",file=hhaxsql)
    print("\tage_id int,",file=hhaxsql)
    print("\tage_name varchar(30),",file=hhaxsql)
    print("\tpostext varchar(60),",file=hhaxsql)
    print("\tpos_id varchar(30),",file=hhaxsql)
    print("\tparent_id varchar(30));",file=hhaxsql)
    print(".mode tabs",file=hhaxsql)
    print(".import hhaxis.txt hhaxis",file=hhaxsql)
    print(".headers on",file=hhaxsql)
    print("select * from hhaxis limit 10;",file=hhaxsql)
    hhaxsql.close()

# This function generates a range of dates
def daterange(start_date, end_date, length=1):
    """ generate a range of dates"""
    for n in range(0, int((end_date - start_date).days), length):
        yield start_date + timedelta(n)

# This function generates weight data
def generateWeightData():
    """ generate weight data """
    filewgts = open("wgt.txt", "w")
    id=config.firsthhid
    start_date = date(2016, 12, 26)
    end_date = date(2020, 1, 5)
    for single_date in daterange(start_date, end_date, 7):
        end_date = single_date + timedelta(6)
        print(single_date.strftime("%Y-%m-%d %A"), end_date.strftime("%Y-%m-%d %A"))
        s=0
        w={}
        for j in range(0, config.hhnr):
            w[j]=random.uniform(0, 1)
            s+=w[j]
        for j in range(0, config.hhnr):
            print(config.firsthhid+j, single_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), w[j]/s*config.hhnr, 1, sep='\t', file=filewgts)
    for byear in range(2017, 2020):
        for bmonth in range(1, 13):
            for eyear in range(byear, 2020):
                if(byear==eyear):
                    semonth=bmonth
                else:
                    semonth=1
                for emonth in range(semonth, 13):
                    begin="{:4d}-{:02d}-{:02d}".format(byear, bmonth, 1)
                    endd=calendar.monthrange(eyear, emonth)[1]
                    end="{:4d}-{:02d}-{:02d}".format(eyear, emonth, endd)
                    print(begin, end)
                    s=0
                    w={}
                    for j in range(0, config.hhnr):
                        w[j]=random.uniform(0, 1)
                        s+=w[j]
                    for j in range(0, config.hhnr):
                        print(config.firsthhid+j, begin, end, w[j]/s*config.hhnr, 0, sep='\t', file=filewgts)
    filewgts.close()

    # genreating the sql file for sqlite to import the data
    wgtsql=open("wgt.sql", "w")
    print("drop table if exists weights;", file=wgtsql)
    print("create table weights (", file=wgtsql)
    print("\thousehold_id int,", file=wgtsql)
    print("\tfrom_date text,", file=wgtsql)
    print("\tto_date text,", file=wgtsql)
    print("\tweight float,", file=wgtsql)
    print("\tstandard int);", file=wgtsql)
    print(".mode tabs", file=wgtsql)
    print(".import wgt.txt weights", file=wgtsql)
    print(".headers on", file=wgtsql)
    print("select * from weights limit 10;", file=wgtsql)
    wgtsql.close()

# This function generates purchase data
def generatePurchaseData():
    """ generate purchase data """
    start_date = date(2017, 1, 1)
    end_date = date(2019, 12, 31)
    plen=int((end_date - start_date).days)
    id=config.firsthhid+config.hhnr
    purfile=open("pur.txt", "w")
    artfeat=[]
    for i in range(config.artfeatnr):
        if i==0:
            nrnv=config.pgcnt
        else:
            nrnv=random.randint(1, 10)
        s=0
        w={}
        for k in range(0, nrnv):
            w[k]=random.uniform(0, 1)
            s+=w[k]
        feat={}
        for k in range(0, nrnv):
            feat[id]=w[k]/s
            id+=1
        artfeat.append(feat)

    pgfeat=[]
    for ipg in range(0, config.pgcnt):
        pgfeat.append([1]+random.sample(range(2, config.artfeatnr), config.featperpg))

    firstshopid=id
    articles=[]
    for i in range(0, config.artcnt):
        article={}
        vp=random.uniform(0, 1)
        s=0
        ipg=0
        for val in artfeat[0]:
            s+=artfeat[0][val]
            if s>=vp:
                break
            ipg+=1
        article[0]=val
        print("article %d: pg %d" % (i, val))
        for ifeat in pgfeat[ipg]:
            vp=random.uniform(0, 1)
            s=0
            for val in artfeat[ifeat]:
                s+=artfeat[ifeat][val]
                if s>=vp:
                    break
            article[ifeat]=val
            #print("article %d: %d %d" % (i, ifeat, val))
            #article volume
        article[-1]=round(2**random.uniform(-1,4),3)
        articles.append(article)

    recid=1000
    for i in range(0, config.hhnr):
        print("hh %d\n" % (i+config.firsthhid))
        nrrec=random.randint(1, config.maxrecperhh)
        for k in range(0, nrrec):
            print(recid, end='\t', file=purfile)
            recid+=1
            print(i+config.firsthhid, end='\t', file=purfile)
            current_date=start_date+timedelta(days=random.randint(0, plen))
            print(current_date, end='\t', file=purfile)
            shop=random.randint(firstshopid, firstshopid+config.shopcnt)
            print(shop, end='\t', file=purfile)
            nr=round(2**random.uniform(-1,5))
            print(nr, end='\t', file=purfile)
            val=nr*round(2**random.uniform(-1,5),2)
            print(val, end='\t', file=purfile)
            article=articles[random.randint(0, config.artcnt-1)]
            vol=nr*article[-1]
            print(vol, end='\t', file=purfile)
            for j in range(0, config.artfeatnr):
                if j in article:
                    #print("%d %d\n" % (j, article[j]))
                    print(article[j], end='\t', file=purfile)
                else:
                    print("NULL", end='\t', file=purfile)
            bf=2**random.uniform(-1,1)
            print(bf, end='\t', file=purfile)
            rw=2**random.uniform(-1,1)
            print(rw, file=purfile)
    purfile.close()

    # now gerenerate the sql file for sqlite to import the data
    pursql=open("pur.sql", "w")
    print("drop table if exists purchases;", file=pursql)
    print("create table purchases (", file=pursql)
    print("\trec_id int,", file=pursql)
    print("\thousehold_id int,", file=pursql)
    print("\tpur_date text,", file=pursql)
    print("\tshop_id int,", file=pursql)
    print("\tpacks int,", file=pursql)
    print("\tvalue float,", file=pursql)
    print("\tvolume float,", file=pursql)
    print("\tcategory_id int,", file=pursql)
    print("\tbrand_id int,", file=pursql)
    for i in range(2, config.artfeatnr):
        print("\tfeat"+str(i)+"_id int default null,", file=pursql)
    print("\tbrand_factor float,", file=pursql)
    print("\trw float);", file=pursql)
    print(".mode tabs", file=pursql)
    print(".import pur.txt purchases", file=pursql)
    print(".headers on", file=pursql)
    print("select * from purchases limit 10;", file=pursql)
    pursql.close()

    # generate the article/purchase axis
    artaxfile=open("artaxis.txt", "w")
    for article in articles:
        print(article[0], article[0], article[1], article[1], article[1], article[1], article[0], sep='\t', file=artaxfile)
        print(article[0], article[0], article[1], article[1], article[0], article[0], "TOTAL", sep='\t', file=artaxfile)
        print(article[0], article[0], article[1], article[1], "TOTAL", "TOTAL", "NULL", sep='\t', file=artaxfile)
    artaxfile.close()

    # generate sql file for sqlite to import the axis data
    artaxsql=open("artaxis.sql", "w")
    print("drop table if exists artaxis;",file=artaxsql)
    print("create table artaxis (",file=artaxsql)
    print("\tcat_id int,",file=artaxsql)
    print("\tcat_name varchar(30),",file=artaxsql)
    print("\tbrnd_id int,",file=artaxsql)
    print("\tbrnd_name varchar(30),",file=artaxsql)
    print("\tpostext varchar(60),",file=artaxsql)
    print("\tpos_id varchar(30),",file=artaxsql)
    print("\tparent_id varchar(30));",file=artaxsql)
    print(".mode tabs",file=artaxsql)
    print(".import artaxis.txt artaxis",file=artaxsql)
    print(".headers on",file=artaxsql)
    print("select * from artaxis limit 10;",file=artaxsql)
    artaxsql.close()


generateHouseholdData()
generateWeightData()
generatePurchaseData()
