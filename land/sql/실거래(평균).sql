
-- 전국 아파트 유니별 평균 구하기(월)
--delete from tblRealStatUnitMon

select *
from tblRealStatUnitMon

--insert into tblRealStatUnitMon
select  sggCd, aptSeq, dealYear, dealMon,  dealAvgAmount,  nm2, p1_code, nm1, aptNm,  cnt , rnk, date('now')
from(
select RANK() OVER (PARTITION BY sggCd, dealYear, dealMon ORDER BY round(avg(dealAvgAmount),0) DESC) AS rnk
      , p1_code, nm1, sggCd, dealYear, dealMon, nm2, aptSeq, aptNm , round(avg(dealAvgAmount),0) dealAvgAmount, sum(cnt) cnt
from(
select c1.p1_code, c2.nm nm1, r.sggCd, strftime('%Y',r.dealDate) dealYear, strftime('%m',dealDate) dealMon
      , c1.nm nm2
      , r.aptSeq, r.aptNm
	  , round(sum(cdealType*dealAmount/excluUseAr)/sum(cdealType)*10000,0) dealAvgAmount, count(*) cnt
from tblReal r left join tblCode c1
					on r.sggCd = c1.code
	           left join tblCode c2
					on c1.p1_code = c2.code
--where r.aptSeq = '11680-4394' 
--and r.dealDate between'2025-01-01' and '2025-12-31'		
--and excluUseAr=94.49		
					--where c1.p1_code ='11000' -- 서울
--and r.dealDate between '2025-04-01' and '2026-06-31'
group by c1.p1_code, r.sggCd, strftime('%Y',dealDate), strftime('%m',dealDate), r.aptSeq
having sum(cdealType*dealAmount/excluUseAr)<>0
)
group by p1_code, nm1, sggCd, dealYear, dealMon, nm2, aptSeq, aptNm, cnt 
--order by dealYear DESC, round(avg(dealAvgAmount),0) desc 
) order by sggCd, dealYear,dealmon,aptSeq, rnk

------------------------------------------------------



--- 피벗 형식

SELECT
    c1.p1_code,
    c2.nm AS nm1,
    r.sggCd,
    c1.nm AS nm2,
    ROUND(AVG(CASE WHEN strftime('%Y', r.dealDate) = '2023' THEN cdealType * dealAmount / excluUseAr * 10000 END), 0) AS "2023",
    ROUND(AVG(CASE WHEN strftime('%Y', r.dealDate) = '2024' THEN cdealType * dealAmount / excluUseAr * 10000 END), 0) AS "2024",
    ROUND(AVG(CASE WHEN strftime('%Y', r.dealDate) = '2025' THEN cdealType * dealAmount / excluUseAr * 10000 END), 0) AS "2025"
FROM tblReal r
LEFT JOIN tblCode c1 ON r.sggCd = c1.code
LEFT JOIN tblCode c2 ON c1.p1_code = c2.code
WHERE c1.p1_code = '11000'  -- 서울
GROUP BY 
    c1.p1_code, c2.nm, r.sggCd, c1.nm
ORDER BY "2024" DESC;





--서울시 구별로 평균
select  RANK() OVER (ORDER BY strftime('%Y',dealDate) desc , round(avg(cdealType*dealAmount/excluUseAr),0) DESC) AS rank
        , c1.p1_code, c2.nm nm1, r.sggCd, strftime('%Y',dealDate) dealDate, c1.nm nm2, avg(round(cdealType*dealAmount/excluUseAr,0)) dealAvgAmount, count(*) cnt
from tblReal r left join tblCode c1
					on r.sggCd = c1.code
	           left join tblCode c2
					on c1.p1_code = c2.code
where c1.p1_code ='11000' -- 서울
--  and r.dealDate between '2024-01-01' and '2024-12-31'
group by c1.p1_code, c2.nm, r.sggCd, strftime('%Y',dealDate), c1.nm 
order by strftime('%Y',dealDate)  desc , round(avg(cdealType*dealAmount/excluUseAr),0) DESC;


