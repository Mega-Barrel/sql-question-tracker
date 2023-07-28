
-- Schema for Raw Table
CREATE TABLE "raw_data" (
	"question_title"	    TEXT,
	"difficulty"			TEXT,
	"created_at"	        TIMESTAMP,
	"platform"	            TEXT,
	"company"	            TEXT []
);

-- Schema for max_date lookup
CREATE TABLE "max_date" (
    "created_at" DATE
);
INSERT INTO 
	max_date 
VALUES(
	'2022-01-01'
);

-- Schema for question solved by companies
CREATE TABLE "companies_solved" (
    "company" TEXT,
	"question_solved" INTEGER
);

-- Schema for question difficulty
CREATE TABLE "ques_difficulty" (
	"difficulty" TEXT,
	"question_solved" INTEGER
);

-- Schema for daily question solved
CREATE TABLE "daily_solved" (
	"created_at" DATE,
	"question_solved" INTEGER
);
