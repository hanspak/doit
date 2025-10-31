--delete from tblRealStatMon


--실거래 년도별통계
create table tblRealStatYear(
	"sggCd" 			TEXT,
	"dealYear" 			TEXT,
	"dealAvgAmount" 	REAL,
	"nm2" 				TEXT,
	"p1_code" 			TEXT,
	"nm1"				TEXT,
	cnt                 INTEGER,	
	"rnk"				INTEGER,
	cratedDate        TEXT,
	PRIMARY KEY("sggCd","dealYear")
);

CREATE INDEX idxRealStatYear ON tblRealStatYear(sggCd,dealYear);	


select *
from tblRealStatMon

--실거래 년도-월별통계
CREATE TABLE tblRealStatMon (
    sggCd            TEXT,
    dealYear         TEXT,
	dealMon         TEXT,
    dealAvgAmount    REAL,
    nm2              TEXT,
    p1_code          TEXT,
    nm1              TEXT,
	cnt              INTEGER,	
    rnk              INTEGER,
    cratedDate        TEXT,
    PRIMARY KEY (sggCd, dealYear, dealMon)
);

CREATE INDEX idxRealStatMon ON tblRealStatMon(sggCd,dealYear,dealMon);	


--실거래 유닛-년도-월별통계
CREATE TABLE tblRealStatUnitYear (
    sggCd            TEXT,
    dealYear         TEXT,
	aptSeq           TEXT,
	excluUseAr       REAL,
    dealAvgAmount    REAL,
    nm2              TEXT,
    p1_code          TEXT,
    nm1              TEXT,
	aptNm            TEXT, 
	cnt              INTEGER,
    rnk              INTEGER,
	cratedDate        TEXT,
    PRIMARY KEY (sggCd, dealYear, aptSeq)
);

CREATE INDEX idxtblRealStatUnitYear ON tblRealStatUnitYear(sggCd,dealYear,aptSeq,excluUseAr);	


--실거래 유닛-년도-월별통계
CREATE TABLE tblRealStatUnitMon (
    sggCd            TEXT,
    dealYear         TEXT,
    dealMon          TEXT,
	aptSeq           TEXT,
	excluUseAr       REAL,	
    dealAvgAmount    REAL,
    nm2              TEXT,
    p1_code          TEXT,
    nm1              TEXT,
	aptNm            TEXT, 
	cnt              INTEGER,
    rnk              INTEGER,
	cratedDate        TEXT,
    PRIMARY KEY (sggCd, dealYear, dealMon, aptSeq,excluUseAr)
);

CREATE INDEX idxtblRealStatUnitMon ON tblRealStatUnitMon(sggCd,dealYear,dealMon,aptSeq,excluUseAr);	



-------------
--실거래 아파트-월별통계
CREATE TABLE tblRealStatAptMon (
    sggCd            TEXT,
    dealYear         TEXT,
    dealMon          TEXT,
	aptSeq           TEXT,
    dealAvgAmount    REAL,
    nm2              TEXT,
    p1_code          TEXT,
    nm1              TEXT,
	aptNm            TEXT, 
	cnt              INTEGER,
    rnk              INTEGER,
	cratedDate        TEXT,
    PRIMARY KEY (sggCd, dealYear, dealMon, aptSeq)
);

CREATE INDEX idxTblRealStatAptMon ON tblRealStatAptMon(sggCd,dealYear,dealMon,aptSeq);	







	