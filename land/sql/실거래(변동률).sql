WITH prev AS (
  SELECT
    rowid AS rid,
    -- LAG로 이전월 가격 가져오기
    LAG(dealAvgAmount) 
      OVER (
        PARTITION BY sggCd, aptSeq
        ORDER BY dealYear, dealMon
      ) AS prev_price,
    dealAvgAmount AS curr_price,
    -- (curr_price - prev_price) / prev_price * 100 계산
    (dealAvgAmount - LAG(dealAvgAmount)
       OVER (
         PARTITION BY sggCd, aptSeq
         ORDER BY dealYear, dealMon
       )
    ) * 100.0
    / NULLIF(
        LAG(dealAvgAmount)
          OVER (
            PARTITION BY sggCd, aptSeq
            ORDER BY dealYear, dealMon
          )
      , 0
      ) AS rate_value
  FROM tblRealStatAptMon
)

-- 2) 업데이트: prev CTE에서 계산된 rate_value를 실제 rate 컬럼에 대입
UPDATE tblRealStatAptMon
SET rate = (
  SELECT rate_value
  FROM prev
  WHERE prev.rid = tblRealStatAptMon.rowid
)
WHERE EXISTS (
  SELECT 1
  FROM prev
  WHERE prev.rid = tblRealStatAptMon.rowid
);

select *
from tblRealStatAptMon a
where dealYear = '2024'

where rate is not null

update tblRealStatAptMon 
 set rate = (
				select (b.dealAvgAmount - b.prev_price)/nullif(b.prev_price,null)
				from(
					select *
						, LAG(dealAvgAmount) OVER (
								PARTITION BY sggCd, aptSeq
								ORDER BY sggCd, aptSeq, dealYear, dealMon
					) AS prev_price
				from tblRealStatAptMon 
				) b
				where sggCd = b.sggCd 
				  and dealYear = b.dealYear
				  and dealMon = b.dealMon
				  and aptSeq = b.aptSeq
              )

			  
select *
from tblRealStatAptMon
group by sggCd, dealYear, dealMon, aptSeq
having count(*)>1			  
			  

SELECT
  a.*,
  -- 이전 월 가격
  LAG(a.dealAvgAmount) OVER (
    PARTITION BY a.sggCd, a.aptSeq
    ORDER BY a.dealYear, a.dealMon
  ) AS prev_price,
  -- 전월 대비 변동률(%) : (현재가–이전가)÷이전가×100
  ROUND(
    (
      a.dealAvgAmount
      - LAG(a.dealAvgAmount) OVER (
          PARTITION BY a.sggCd, a.aptSeq
          ORDER BY a.dealYear, a.dealMon
        )
    ) * 100.0
    / NULLIF(
        LAG(a.dealAvgAmount) OVER (
          PARTITION BY a.sggCd, a.aptSeq
          ORDER BY a.dealYear, a.dealMon
        )
      , 0
      )
  , 2) AS rate
FROM tblRealStatAptMon AS a
ORDER BY
  a.sggCd,
  a.aptSeq,
  a.dealYear,
  a.dealMon;








-- 1) prev CTE: 각 행의 이전월 가격과 변동률 계산
WITH prev AS (
  SELECT
    rowid AS rid,
    LAG(dealAvgAmount) OVER (
      PARTITION BY sggCd, aptSeq
      ORDER BY dealYear, dealMon
    ) AS prev_price,
    -- (현재가-이전가) ÷ 이전가 × 100
    (dealAvgAmount
       - LAG(dealAvgAmount) OVER (
           PARTITION BY sggCd, aptSeq
           ORDER BY dealYear, dealMon
         )
    ) * 100.0
    / NULLIF(
        LAG(dealAvgAmount) OVER (
          PARTITION BY sggCd, aptSeq
          ORDER BY dealYear, dealMon
        )
      , 0
      ) AS rate_val
  FROM tblRealStatAptMon
)

-- 2) 계산된 rate_val을 실제 rate 컬럼에 업데이트
UPDATE tblRealStatAptMon
SET rate = (
  SELECT rate_val
  FROM prev
  WHERE prev.rid = tblRealStatAptMon.rowid
)
WHERE EXISTS (
  SELECT 1
  FROM prev
  WHERE prev.rid = tblRealStatAptMon.rowid
);
