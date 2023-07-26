
-- Schema for Raw Table
CREATE TABLE "raw_data" (
	"question_title"	    TEXT,
	"question_difficulty"	TEXT,
	"created_at"	        TIMESTAMP,
	"platform"	            TEXT,
	"company"	            TEXT []
);

-- Schema for max_date lookup
CREATE TABLE "max_date" (
    "created_at" TIMESTAMP
);

-- Schema for question solved by companies
CREATE TABLE "companies_solved" (
    "platform" TEXT,
	"question_solved" INTEGER
);

-- Schema for question difficulty
CREATE TABLE "ques_difficulty" (
	"difficulty" TEXT,
	"question_solved" INTEGER
)
