WITH bounds AS (
  SELECT
    DATE(MIN(dealYear || '-' || dealMon || '-01')) AS start_dt,
    DATE(MAX(dealYear || '-' || dealMon || '-01')) AS end_dt
  FROM tblRealStatAptMon
),
months(m) AS (
  SELECT start_dt FROM bounds
  UNION ALL
  SELECT DATE(m, '+1 month')
  FROM months, bounds
  WHERE m < end_dt
),
apts AS (
  SELECT DISTINCT sggCd, aptSeq, p1_code, nm1, nm2, aptNm
  FROM tblRealStatAptMon
),
target AS (
  SELECT
    a.sggCd,
    a.aptSeq,
    a.p1_code,
    a.nm1,
    a.nm2,
    a.aptNm,
    STRFTIME('%Y', m) AS dealYear,
    STRFTIME('%m', m) AS dealMon
  FROM apts AS a
  CROSS JOIN months AS m
),
joined AS (
  SELECT
    t.*,
    r.dealAvgAmount,
    r.cnt,
    r.rnk
  FROM target AS t
  LEFT JOIN tblRealStatAptMon AS r
    ON t.sggCd    = r.sggCd
   AND t.aptSeq  = r.aptSeq
   AND t.dealYear= r.dealYear
   AND t.dealMon = r.dealMon
),
filled AS (
  SELECT
    *,
    COALESCE(dealAvgAmount,
      LAG(dealAvgAmount) OVER (
        PARTITION BY sggCd, aptSeq
        ORDER BY dealYear, dealMon
      )
    ) AS filledAvg,
    COALESCE(cnt,
      LAG(cnt) OVER (
        PARTITION BY sggCd, aptSeq
        ORDER BY dealYear, dealMon
      )
    ) AS filledCnt,
    COALESCE(rnk,
      LAG(rnk) OVER (
        PARTITION BY sggCd, aptSeq
        ORDER BY dealYear, dealMon
      )
    ) AS filledRnk
  FROM joined
)
INSERT OR IGNORE INTO tblRealStatAptMon (
  sggCd, aptSeq, p1_code, nm1, nm2, aptNm,
  dealYear, dealMon, dealAvgAmount, cnt, rnk, createdDate
)
SELECT
  sggCd,
  aptSeq,
  p1_code,
  nm1,
  nm2,
  aptNm,
  dealYear,
  dealMon,
  filledAvg    AS dealAvgAmount,
  filledCnt    AS cnt,
  filledRnk    AS rnk,
  DATETIME('now','localtime') AS createdDate
FROM filled
WHERE dealAvgAmount IS NULL  -- 기존에 값이 없던 월만 선택
;
