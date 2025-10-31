
select *
from tblRealStatAptMon m
where m.dealYear='2025'
--and m.aptSeq = '11680-4394' 


select *
update tblRealStatAptMon
  set cnt = 0
where length(createdDate)>18



select * 
from tblRealStatAptMon m
where m.dealYear = '2025'
and m.dealMon in ('02')
and not exists( select 1 
			from tblRealStatAptMon n
			where n.sggCd = m.sggCd
			and n.dealYear = m.dealYear
			and n.dealMon in ('03')
			and n.aptSeq = m.aptSeq
		   )	






SELECT  m.p1_code, m.sggCd,  m.aptSeq, m.nm1, m.nm2, m.aptNm
      ,d1. dealYear, d1.dealMon, d1.dealAvgAmount, d1.rnk, round((d1.dealAvgAmount - d2.dealAvgAmount)/d2.dealAvgAmount*100,1) rate1
	  ,d2. dealYear, d2.dealMon, d2.dealAvgAmount, d2.rnk, round((d2.dealAvgAmount - d3.dealAvgAmount)/d3.dealAvgAmount*100,1) rate2 
	  ,d3. dealYear, d3.dealMon, d3.dealAvgAmount, d3.rnk, round((d3.dealAvgAmount - d2.dealAvgAmount)/d2.dealAvgAmount*100,1) rate3
	  ,d4. dealYear, d4.dealMon, d4.dealAvgAmount, d4.rnk
FROM
  (
	SELECT max(dealYear) dealYear, max(dealMon) dealMon, max(sggCd) sggCd, max(aptSeq) aptSeq, max(p1_code) p1_code
	 	 , max(nm1) nm1, max(nm2) nm2, max(aptNm) aptNm, sum(cnt) cnt
	FROM tblRealStatAptMon
	where dealYear >='2025'
	and dealMon = '03'
	group by dealYear, aptSeq
  ) AS m  left JOIN   (
			SELECT *
			FROM tblRealStatAptMon
			WHERE dealYear = strftime('%Y', date('now','localtime','-3 month'))
			  AND dealMon  = strftime('%m', date('now','localtime','-3 month'))
           ) AS d1 
			 ON m.sggCd   = d1.sggCd
			 AND m.aptSeq = d1.aptSeq
			 and m.p1_code = d1.p1_code
		 left JOIN   (
			SELECT *
			FROM tblRealStatAptMon
			WHERE dealYear = strftime('%Y', date('now','localtime','-4 month'))
			  AND dealMon  = strftime('%m', date('now','localtime','-4 month'))
		  ) AS d2
		  ON m.sggCd   = d2.sggCd
		 AND m.aptSeq = d2.aptSeq
		 and m.p1_code = d2.p1_code 
		left JOIN   (
			SELECT *
			FROM tblRealStatAptMon
			WHERE dealYear = strftime('%Y', date('now','localtime','-5 month'))
			  AND dealMon  = strftime('%m', date('now','localtime','-5 month'))
		  ) AS d3
		  ON m.sggCd   = d3.sggCd
		 AND m.aptSeq = d3.aptSeq
		 and m.p1_code = d3.p1_code 		 
        left JOIN   (
			SELECT *
			FROM tblRealStatAptMon
			WHERE dealYear = strftime('%Y', date('now','localtime','-6 month'))
			  AND dealMon  = strftime('%m', date('now','localtime','-6 month'))
		  ) AS d4
		  ON m.sggCd   = d4.sggCd
		 AND m.	  = d4.aptSeq
		 and m.p1_code = d4.p1_code 		 
where m.aptSeq = '11680-4394' 
and m.dealYear in ('2025','2024','2023')


 


WITH base AS (
  SELECT     sggCd,     aptSeq,     p1_code,     nm1,     nm2,
			 aptNm,     dealYear,    dealMon,     dealAvgAmount,    rnk,
    -- YYYY*12 + MM 계산 후 현재 기준(YYYY*12 + MM) 차이 구하기
    (CAST(dealYear AS INTEGER) * 12 + CAST(dealMon AS INTEGER))
    - (CAST(strftime('%Y', date('now','localtime')) AS INTEGER) * 12
       + CAST(strftime('%m', date('now','localtime')) AS INTEGER))
      AS months_diff
  FROM tblRealStatAptMon
  WHERE dealYear >= '2020'
)
SELECT   sggCd,   aptSeq,   p1_code,   nm1,   nm2,   aptNm,   -- 1개월 전
  MAX(CASE WHEN months_diff = -1 THEN dealYear     END) AS y_1m,
  MAX(CASE WHEN months_diff = -1 THEN dealMon      END) AS m_1m,
  MAX(CASE WHEN months_diff = -1 THEN dealAvgAmount END) AS avg_1m,
  MAX(CASE WHEN months_diff = -1 THEN rnk          END) AS rnk_1m,

  -- 2개월 전
  MAX(CASE WHEN months_diff = -2 THEN dealYear     END) AS y_2m,
  MAX(CASE WHEN months_diff = -2 THEN dealMon      END) AS m_2m,
  MAX(CASE WHEN months_diff = -2 THEN dealAvgAmount END) AS avg_2m,
  MAX(CASE WHEN months_diff = -2 THEN rnk          END) AS rnk_2m
/*
  -- 3개월 전
  MAX(CASE WHEN months_diff = -3 THEN dealYear     END) AS y_3m,
  MAX(CASE WHEN months_diff = -3 THEN dealMon      END) AS m_3m,
  MAX(CASE WHEN months_diff = -3 THEN dealAvgAmount END) AS avg_3m,
  MAX(CASE WHEN months_diff = -3 THEN rnk          END) AS rnk_3m,

  -- 4개월 전
  MAX(CASE WHEN months_diff = -4 THEN dealYear     END) AS y_4m,
  MAX(CASE WHEN months_diff = -4 THEN dealMon      END) AS m_4m,
  MAX(CASE WHEN months_diff = -4 THEN dealAvgAmount END) AS avg_4m,
  MAX(CASE WHEN months_diff = -4 THEN rnk          END) AS rnk_4m,

  -- 5개월 전
  MAX(CASE WHEN months_diff = -5 THEN dealYear     END) AS y_5m,
  MAX(CASE WHEN months_diff = -5 THEN dealMon      END) AS m_5m,
  MAX(CASE WHEN months_diff = -5 THEN dealAvgAmount END) AS avg_5m,
  MAX(CASE WHEN months_diff = -5 THEN rnk          END) AS rnk_5m,

  -- 6개월 전
  MAX(CASE WHEN months_diff = -6 THEN dealYear     END) AS y_6m,
  MAX(CASE WHEN months_diff = -6 THEN dealMon      END) AS m_6m,
  MAX(CASE WHEN months_diff = -6 THEN dealAvgAmount END) AS avg_6m,
  MAX(CASE WHEN months_diff = -6 THEN rnk          END) AS rnk_6m,

  -- 7개월 전
  MAX(CASE WHEN months_diff = -7 THEN dealYear     END) AS y_7m,
  MAX(CASE WHEN months_diff = -7 THEN dealMon      END) AS m_7m,
  MAX(CASE WHEN months_diff = -7 THEN dealAvgAmount END) AS avg_7m,
  MAX(CASE WHEN months_diff = -7 THEN rnk          END) AS rnk_7m,

  -- 8개월 전
  MAX(CASE WHEN months_diff = -8 THEN dealYear     END) AS y_8m,
  MAX(CASE WHEN months_diff = -8 THEN dealMon      END) AS m_8m,
  MAX(CASE WHEN months_diff = -8 THEN dealAvgAmount END) AS avg_8m,
  MAX(CASE WHEN months_diff = -8 THEN rnk          END) AS rnk_8m,

  -- 9개월 전
  MAX(CASE WHEN months_diff = -9 THEN dealYear     END) AS y_9m,
  MAX(CASE WHEN months_diff = -9 THEN dealMon      END) AS m_9m,
  MAX(CASE WHEN months_diff = -9 THEN dealAvgAmount END) AS avg_9m,
  MAX(CASE WHEN months_diff = -9 THEN rnk          END) AS rnk_9m,

  -- 10개월 전
  MAX(CASE WHEN months_diff = -10 THEN dealYear     END) AS y_10m,
  MAX(CASE WHEN months_diff = -10 THEN dealMon      END) AS m_10m,
  MAX(CASE WHEN months_diff = -10 THEN dealAvgAmount END) AS avg_10m,
  MAX(CASE WHEN months_diff = -10 THEN rnk          END) AS rnk_10m
*/
FROM base
GROUP BY
  sggCd,
  aptSeq,
  p1_code,
  nm1,
  nm2,
  aptNm
ORDER BY
  sggCd,
  aptSeq;



WITH base AS (
  SELECT
    sggCd, aptSeq, p1_code, nm1, nm2,
    aptNm, dealYear, dealMon, dealAvgAmount, rnk,
    -- YYYY*12 + MM 계산
    (CAST(dealYear AS INTEGER) * 12 + CAST(dealMon AS INTEGER))
    - (CAST(strftime('%Y', date('now','localtime')) AS INTEGER) * 12
       + CAST(strftime('%m', date('now','localtime')) AS INTEGER))
      AS months_diff
  FROM tblRealStatAptMon
  WHERE dealYear >= '2020'
)
SELECT
  sggCd, aptSeq, p1_code, nm1, nm2, aptNm,
  -- 현재 기준(0개월 차)
  MAX(CASE WHEN months_diff = 0 THEN dealYear END)          AS cur_year,
  MAX(CASE WHEN months_diff = 0 THEN dealMon END)           AS cur_mon,
  MAX(CASE WHEN months_diff = 0 THEN dealAvgAmount END)     AS cur_avg,
  MAX(CASE WHEN months_diff = 0 THEN rnk END)               AS cur_rnk,
  -- 1개월 전
  MAX(CASE WHEN months_diff = -1 THEN dealYear END)         AS m3_year,
  MAX(CASE WHEN months_diff = -1 THEN dealMon END)          AS m3_mon,
  MAX(CASE WHEN months_diff = -1 THEN dealAvgAmount END)    AS m3_avg,
  MAX(CASE WHEN months_diff = -1 THEN rnk END)              AS m3_rnk,
  -- 2개월 전
  MAX(CASE WHEN months_diff = -2 THEN dealYear END)         AS m4_year,
  MAX(CASE WHEN months_diff = -2 THEN dealMon END)          AS m4_mon,
  MAX(CASE WHEN months_diff = -2 THEN dealAvgAmount END)    AS m4_avg,
  MAX(CASE WHEN months_diff = -2 THEN rnk END)              AS m4_rnk
  -- 3개월 전
  MAX(CASE WHEN months_diff = -3 THEN dealYear END)         AS m3_year,
  MAX(CASE WHEN months_diff = -3 THEN dealMon END)          AS m3_mon,
  MAX(CASE WHEN months_diff = -3 THEN dealAvgAmount END)    AS m3_avg,
  MAX(CASE WHEN months_diff = -3 THEN rnk END)              AS m3_rnk,
  -- 4개월 전
  MAX(CASE WHEN months_diff = -4 THEN dealYear END)         AS m4_year,
  MAX(CASE WHEN months_diff = -4 THEN dealMon END)          AS m4_mon,
  MAX(CASE WHEN months_diff = -4 THEN dealAvgAmount END)    AS m4_avg,
  MAX(CASE WHEN months_diff = -4 THEN rnk END)              AS m4_rnk
  -- 5개월 전
  MAX(CASE WHEN months_diff = -5 THEN dealYear END)         AS m3_year,
  MAX(CASE WHEN months_diff = -5 THEN dealMon END)          AS m3_mon,
  MAX(CASE WHEN months_diff = -5 THEN dealAvgAmount END)    AS m3_avg,
  MAX(CASE WHEN months_diff = -5 THEN rnk END)              AS m3_rnk,
  -- 6개월 전
  MAX(CASE WHEN months_diff = -6 THEN dealYear END)         AS m4_year,
  MAX(CASE WHEN months_diff = -6 THEN dealMon END)          AS m4_mon,
  MAX(CASE WHEN months_diff = -6 THEN dealAvgAmount END)    AS m4_avg,
  MAX(CASE WHEN months_diff = -6 THEN rnk END)              AS m4_rnk
    -- 7개월 전
  MAX(CASE WHEN months_diff = -7 THEN dealYear END)         AS m3_year,
  MAX(CASE WHEN months_diff = -7 THEN dealMon END)          AS m3_mon,
  MAX(CASE WHEN months_diff = -7 THEN dealAvgAmount END)    AS m3_avg,
  MAX(CASE WHEN months_diff = -7 THEN rnk END)              AS m3_rnk,
  -- 8개월 전
  MAX(CASE WHEN months_diff = -8 THEN dealYear END)         AS m4_year,
  MAX(CASE WHEN months_diff = -8 THEN dealMon END)          AS m4_mon,
  MAX(CASE WHEN months_diff = -8 THEN dealAvgAmount END)    AS m4_avg,
  MAX(CASE WHEN months_diff = -8 THEN rnk END)              AS m4_rnk
    -- 9개월 전
  MAX(CASE WHEN months_diff = -9 THEN dealYear END)         AS m3_year,
  MAX(CASE WHEN months_diff = -9 THEN dealMon END)          AS m3_mon,
  MAX(CASE WHEN months_diff = -9 THEN dealAvgAmount END)    AS m3_avg,
  MAX(CASE WHEN months_diff = -9 THEN rnk END)              AS m3_rnk,
  -- 10개월 전
  MAX(CASE WHEN months_diff = -10 THEN dealYear END)         AS m4_year,
  MAX(CASE WHEN months_diff = -10 THEN dealMon END)          AS m4_mon,
  MAX(CASE WHEN months_diff = -10 THEN dealAvgAmount END)    AS m4_avg,
  MAX(CASE WHEN months_diff = -10 THEN rnk END)              AS m4_rnk
  FROM base
WHERE months_diff IN (0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10)
and aptSeq = '11680-4394'
GROUP BY sggCd, aptSeq, p1_code, nm1, nm2, aptNm
ORDER BY sggCd, aptSeq;









select strftime('%Y', date('now','localtime','-3 month'))
from tblReal



SELECT max(dealYear) dealYear, max(dealMon) dealMon, max(sggCd) sggCd, max(aptSeq) aptSeq, max(p1_code) p1_code
     , max(nm1) nm1, max(nm2) nm2, max(aptNm) aptNm, sum(cnt) cnt
FROM tblRealStatAptMon
group by aptSeq

having sum(cnt) > 100

select *
from tblRealStatAptMon
where dealYear = '2025'
and dealMon = '04'



	   
	
	
	
	left outer join (
	select *
	from tblRealStatMon 
	where dealYear = strftime('%Y', date('now', 'localtime', '-4 month')) 
	and dealMon = strftime('%m', date('now', 'localtime', '-4 month')) 
	)d2	
	
	
	--printf('%02d',date('now'))




m outer left join tblRealStatMon d1
	on m.sggCd = d1.sggCd
	and m.p1_code1 = d1.p1_code1
	and m.dealYear = d1.dealYear
	and m.dealMon =  strftime('%Y', date('now'))
	and m.deal
	
	



select *
from tblRealStatMon

where sggCd = '11680'
and aptSeq = '11680-4394' 
and dealYear = '2025'

select *
from tblReal
where aptSeq = '11680-4394' 
and dealDate between'2025-01-01' and '2025-12-31'

and excluUseAr=94.49

--delete from tblRealStatUnitYear

select a.sggCd, a.dealYear, a.aptSeq, a.dealAvgAmount, a.nm2, a.p1_code, a.nm1, a.aptNm, a.cnt, a.rnk
from tblRealStatUnitYear a
where dealYear >='2015'
and rnk <=5
order by rnk, dealYear


SELECT  m.sggCd,  m.aptSeq, d.nm1,  d.nm2,  d.aptNm,
  MAX(CASE WHEN d.dealYear = '2015' THEN d.dealAvgAmount END) AS "2015",
  MAX(CASE WHEN d.dealYear = '2015' THEN d.rnk END) AS "2015rnk",
  MAX(CASE WHEN d.dealYear = '2016' THEN d.dealAvgAmount END) AS "2016",
  MAX(CASE WHEN d.dealYear = '2016' THEN d.rnk END) AS "2016rnk",
  MAX(CASE WHEN d.dealYear = '2017' THEN d.dealAvgAmount END) AS "2017",
  MAX(CASE WHEN d.dealYear = '2017' THEN d.rnk END) AS "2017rnk",
  MAX(CASE WHEN d.dealYear = '2018' THEN d.dealAvgAmount END) AS "2018",
  MAX(CASE WHEN d.dealYear = '2018' THEN d.rnk END) AS "2018rnk",
  MAX(CASE WHEN d.dealYear = '2019' THEN d.dealAvgAmount END) AS "2019",
  MAX(CASE WHEN d.dealYear = '2019' THEN d.rnk END) AS "2019rnk",
  MAX(CASE WHEN d.dealYear = '2020' THEN d.dealAvgAmount END) AS "2020",
  MAX(CASE WHEN d.dealYear = '2020' THEN d.rnk END) AS "2020rnk",
  MAX(CASE WHEN d.dealYear = '2021' THEN d.dealAvgAmount END) AS "2021",
  MAX(CASE WHEN d.dealYear = '2021' THEN d.rnk END) AS "2021rnk",
  MAX(CASE WHEN d.dealYear = '2022' THEN d.dealAvgAmount END) AS "2022",
  MAX(CASE WHEN d.dealYear = '2022' THEN d.rnk END) AS "2022rnk",
  MAX(CASE WHEN d.dealYear = '2023' THEN d.dealAvgAmount END) AS "2023",
  MAX(CASE WHEN d.dealYear = '2023' THEN d.rnk END) AS "2023rnk",
  MAX(CASE WHEN d.dealYear = '2024' THEN d.dealAvgAmount END) AS "2024",
  MAX(CASE WHEN d.dealYear = '2024' THEN d.rnk END) AS "2024rnk",
  MAX(CASE WHEN d.dealYear = '2025' THEN d.dealAvgAmount END) AS "2025",  
  MAX(CASE WHEN d.dealYear = '2025' THEN d.rnk END) AS "2025rnk"
from(
	select sggCd, aptSeq
	FROM tblRealStatUnitYear 
	WHERE dealYear = '2024' 
	and nm2 = '종로'
	and rnk <=5
	order by rnk 
) m left join  tblRealStatUnitYear d
    on m.sggCd = d.sggCd
   and m.aptSeq = d.aptSeq
WHERE d.dealYear >= '2015'
GROUP BY d.p1_code, d.sggCd, d.aptSeq 
ORDER BY d.nm1,d.nm2,d.aptNm;   



SELECT   sggCd,  aptSeq, nm1,  nm2,  aptNm,
  CASE WHEN dealYear = '2015' THEN dealAvgAmount END AS "2015",
  MAX(CASE WHEN dealYear = '2016' THEN dealAvgAmount END) AS "2016",
  MAX(CASE WHEN dealYear = '2017' THEN dealAvgAmount END) AS "2017",
  MAX(CASE WHEN dealYear = '2018' THEN dealAvgAmount END) AS "2018",
  MAX(CASE WHEN dealYear = '2019' THEN dealAvgAmount END) AS "2019",
  MAX(CASE WHEN dealYear = '2020' THEN dealAvgAmount END) AS "2020",
  MAX(CASE WHEN dealYear = '2021' THEN dealAvgAmount END) AS "2021",
  MAX(CASE WHEN dealYear = '2022' THEN dealAvgAmount END) AS "2022",
  MAX(CASE WHEN dealYear = '2023' THEN dealAvgAmount END) AS "2023",
  MAX(CASE WHEN dealYear = '2024' THEN dealAvgAmount END) AS "2024"
FROM tblRealStatUnitYear
WHERE dealYear >= '2015' AND rnk <= 10
and nm2 = '중랑'
GROUP BY p1_code, sggCd, aptSeq 
ORDER BY nm1,nm2,aptNm;