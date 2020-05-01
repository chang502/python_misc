tmax=30000
f=open('csv_1.csv')
count=0
lines=f.readlines()[1:]
f.close()


sqlquery='''UNLOAD ('
    select aw_publisher, make_code, model_code, zipcode, year_and_month, clicks, queries, advertiser_spent from (
    SELECT
    dp.aw_publisher,
    dmm.make_code,
    dmm.model_code,
     case when dg.zipcode ~ ''^[0-9]+$'' then dg.zipcode else null end::int as zipcode,
    dt.year_and_month,
    sum(vfpe.clicks) AS clicks,
    sum(vfpe.queries) AS queries,
    sum(vfpe.advertiser_spent) AS advertiser_spent
FROM facts.vw_fct_publisher_events vfpe
    JOIN dimensions.dim_make_model dmm ON vfpe.fk_make_model = dmm.pk_make_model
    JOIN dimensions.dim_geography dg ON dg.pk_geography = vfpe.fk_geography
    JOIN dimensions.dim_time dt ON dt.pk_time = vfpe.fk_time_est AND dt.pk_time >= 20190401 and dt.pk_time <= 20200331
    JOIN dimensions.dim_publisher dp ON dp.pk_publisher = vfpe.fk_publisher
    JOIN dimensions.dim_product_category dpc ON dpc.pk_product_category = vfpe.fk_product_category and dpc.aw_product_category=1
    JOIN dimensions.dim_device dd ON dd.pk_device = vfpe.fk_device
    JOIN dimensions.dim_target dt2 ON dt2.pk_target = vfpe.fk_target
WHERE vfpe.flag_is_billable = 1
    AND dt2.aw_target = 0
    AND REGEXP_COUNT(dg.zipcode, ''^[0-9]+$'') > 0
    AND dmm.make_code NOT IN (SELECT dmm.make_code
FROM facts.vw_fct_publisher_events vfpe
    JOIN dimensions.dim_make_model dmm ON vfpe.fk_make_model = dmm.pk_make_model
WHERE vfpe.fk_time_est >= 20190401 and vfpe.fk_time_est <= 20200331
    AND vfpe.flag_is_billable = 1
GROUP BY 1
HAVING sum(vfpe.clicks) < 400
    )
GROUP BY 1,2,3,4,5)
where zipcode between {lower} and {upper}
ORDER BY 1,2,3,4,5')
TO 's3://.../forecasting/splitted/fc_1_{fileNamePrefix}_'
ACCESS_KEY_ID 'A...Q'
SECRET_ACCESS_KEY '0...4'
FORMAT AS CSV
HEADER GZIP
PARALLEL OFF;

'''





results=[]
count=0
prevz='0'
limcz='0'
maxval=0
for lin in lines:
    ln=lin[:-1].split(',')
    #print(ln)
    count+=int(ln[1])
    if count>=tmax:
        prevz=limcz
        limcz=ln[0]
        if count>maxval:
            maxval=count
        count=0
        results.append([prevz,limcz])
        
for i in results:
    print(i)
print(len(results))
print(maxval)



fw=open('script_splitted.sql','w')
count=0
for i in results:
    fw.write(sqlquery.format(lower=i[0],upper=i[1],fileNamePrefix=count))
    count+=1
fw.close()




fscript=open('download_splitted_1_files.bat','w',newline='')

for i in range(0,len(results)):
    fscript.write('aws s3 cp s3://.../forecasting/splitted/fc_1_{}_000.gz ./splitted/fc_1_{}_000.gz\r\n'.format(i,i))
fscript.write('pause\r\n')
fscript.close()    






consoles=5
writers=[]
for i in range(1,consoles+1):
    writers.append(open('console_{}.bat'.format(i),'w',newline=''))
    pass

cont_wrtr=0
for i in range(16,100):
    writers[cont_wrtr].write('python3 clickForecasting.py splitted\\fc_1_{}_000.gz\r\n'.format(i))
    cont_wrtr+=1
    if cont_wrtr==5:
        cont_wrtr=0
        
for wr in writers:
    wr.write('pause\r\n')
    wr.close()
