select *
--delete
from tbl_real_estate
where 계약년도 = '2025'
and 계약월 in ('04','05','06')



with real_5year as(
	select sggCd, aptSeq
		 , round(avg(dealAvgAmount),0) as avgTotAmount
		 , round(avg(rnk),0) as avgTotRnk
	from tblRealStatAptMon 
    where dealYear >= strftime('%Y', date('now','localtime','-5 year'))
    group by sggCd, aptSeq 
)
,
real_1year as(
	select sggCd, aptSeq, aptNm, nm2, nm1, dealYear,  dealMon, dealAvgAmount
		  , avg(dealAvgAmount)  over ( partition by sggCd, aptSeq, dealYear
     								   order by sggCd, aptSeq, dealYear ) avgAmount
		  , avg(rnk)  over ( partition by sggCd, aptSeq, dealYear
								       order by sggCd, aptSeq, dealYear ) avgRnk
	from tblRealStatAptMon
	where dealYear >= strftime('%Y', date('now','localtime','-2 year'))
	
)
select a.*, b.*
from real_1year as a left join real_5year as b
				  on a.sggCd = b.sggCd
				 and a.aptSeq = b.aptSeq



select sggCd, aptSeq, aptNm, nm2, nm1, dealYear,  dealMon, dealAvgAmount
     , avg(dealAvgAmount)  over ( partition by sggCd, aptSeq, dealYear
	                         order by sggCd, aptSeq, dealYear ) avgAmount
     , avg(rnk)  over ( partition by sggCd, aptSeq, dealYear
	                         order by sggCd, aptSeq, dealYear ) avgRnk
     , avg(dealAvgAmount)  over ( partition by sggCd, aptSeq
	                         order by sggCd, aptSeq ) avgTotAmount
     , avg(rnk)  over ( partition by sggCd, aptSeq
	                         order by sggCd, aptSeq ) avgTotRnk
from tblRealStatAptMon 
order by sggCd, aptSeq , dealYear, dealMon


WITH real_5year AS (
  SELECT
    sggCd,
    aptSeq,
    MAX(aptNm)   AS aptNm,
    MAX(nm2)     AS nm2,
    MAX(nm1)     AS nm1,
    ROUND(AVG(dealAvgAmount), 0) AS avgTotAmount,
    ROUND(AVG(rnk),           0) AS avgTotRnk
  FROM tblRealStatAptMon
  WHERE dealYear >= strftime('%Y', date('now','localtime','-5 year'))
  GROUP BY sggCd, aptSeq
),

real_1year AS (
  SELECT
    sggCd,
    aptSeq,
    aptNm,
    nm2,
    nm1,
    dealYear,
    dealMon,
    dealAvgAmount,
    ROUND(AVG(dealAvgAmount) OVER (
      PARTITION BY sggCd, aptSeq, dealYear
    ), 0) AS avgAmount,
    ROUND(AVG(rnk) OVER (
      PARTITION BY sggCd, aptSeq, dealYear
    ), 0) AS avgRnk
  FROM tblRealStatAptMon
  WHERE dealYear >= strftime('%Y', date('now','localtime','-1 year'))
)

SELECT
  a.*,
  b.avgTotAmount,
  b.avgTotRnk
FROM real_1year AS a
LEFT JOIN real_5year AS b
  ON a.sggCd   = b.sggCd
 AND a.aptSeq  = b.aptSeq
ORDER BY
  a.sggCd,
  a.aptSeq,
  a.dealYear,
  a.dealMon;
