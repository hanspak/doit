with dealCount1 as(
/*
시도 거래량 조회
*/
SELECT p1_code, dealYear, dealMon, nm1, cnt1,cnt1_max, cnt1_min, cnt1_total, round((cnt1_max*1.0/cnt1_total*1.0*100),2) cnt1_max_rate
FROM (
	SELECT p1_code, dealYear, dealMon, max(nm1) nm1, SUM(cnt) AS cnt1,
		RANK() OVER (partition by p1_code  ORDER BY SUM(cnt) DESC) AS cnt1_max,
		RANK() OVER (partition by p1_code  ORDER BY SUM(cnt) ASC) AS cnt1_min,
        count(*) OVER (PARTITION BY p1_code) AS cnt1_total
		FROM tblRealStatUnitMon
	GROUP BY p1_code, dealYear, dealMon
    )
--where p1_code = '11000'
--and dealYear = '2025'
--and dealMon = '03'
--order by  cnt1_max desc, p1_code, dealYear, dealMon
),
dealCount2 as(
/*
시군구 거래량 조회
*/
SELECT p1_code, sggCd, dealYear, dealMon, nm1, nm2, cnt2, cnt2_max, cnt2_min, cnt2_total, round((cnt2_max*1.0/cnt2_total*1.0*100),2) cnt2_max_rate
FROM (
	SELECT 	p1_code, sggCd, dealYear, dealMon, 	max(nm1) nm1,
		max(nm2) nm2, SUM(cnt) AS cnt2,
		RANK() OVER (partition by p1_code, sggCd ORDER BY SUM(cnt) DESC) AS cnt2_max,
		RANK() OVER (partition by p1_code, sggCd ORDER BY SUM(cnt) ASC) AS cnt2_min,
		count(*) OVER (PARTITION BY p1_code, sggCd) AS cnt2_total
	FROM tblRealStatUnitMon
	GROUP BY p1_code, sggCd, dealYear, dealMon
    )
--where p1_code = '11000'
--and nm2 = '강남'
--order by cnt2_max ASC, p1_code, sggCd, dealYear, dealMon
)
select a.*, b.cnt1_max, b.cnt1_min,  b.cnt1_total, b.cnt1_max_rate
from dealCount2 a left join dealCount1 b
     on a.p1_code=b.p1_code
	and a.dealYear = b.dealYear
    and a.dealMon = b.dealMon
where a.p1_code = '27000'
and a.nm2 = '수성'
order by a.p1_code, a.sggCd, a.dealYear, a.dealMon	
	
	



select *
FROM tblRealStatUnitMon


select a.rowid, a.*
from tblCode a 
where code in ('41130','41135','41273','41270','41000')
--where nm in ( '성남', '안산')

41000 경기도
41130 성남     41000
41135 분당     41000 41130
41270 안산     41000
41273 단원     41000 41270

select *
from tblCode

update tblCode
  set p1_code = '41000', p2_code = '41270'
where p1_code ='41270'


select *
from tblCode
where nm in ('분당','단원')



