USE DB1;

-- CREATE A COPY OF THE ORIGINAL DATASET TO DO ANALYSIS 
-- AND DOING CHANGES TO THE DATA SO RAW DATA SHOULD BE SAFE IN ORDER TO DO ANY BLUNDER
CREATE TABLE ipl_staging LIKE ipl;

-- INSERT DATA IN STAGEING TABLE FROM ORIGINAL TABLE 
INSERT INTO ipl_staging
SELECT *
FROM ipl;

-- VISUALIZATION OF THE DATA

SELECT * 
FROM ipl_staging;

-- CHECK WHETHER THE TABLE CONTAIN DUPLICATES USING COUNT FUNCTIONS AND SPECIFYING PRIMARY KEY (PK)
SELECT
ID,innings,overs,ballnumber,
COUNT(*) OVER(PARTITION BY ID,innings,overs,ballnumber) AS PK
FROM ipl_staging;

SELECT *
FROM (SELECT
ID,innings,overs,ballnumber,
COUNT(*) OVER(PARTITION BY ID,innings,overs,ballnumber) AS PK
FROM ipl_staging
)t WHERE PK > 1;
-- no duplicate rows in the data


-- TOP 10 BATSMAN BY TOTAL RUNS IN THE IPL FROM 2008 TO 2023 

SELECT batter,SUM(total_run)
FROM ipl_staging
GROUP BY batter
ORDER BY 2 DESC LIMIT 10;

-- EVERY CENTURIES WITH THE BATSMAN NAME AND THE SCORE
SELECT batter,SUM(batsman_run) as score
FROM ipl_staging
GROUP BY batter,ID
HAVING score >= 100
ORDER BY 2 DESC;

-- TOP 10 BATSMAN HAVING MOST CENTURIES

WITH Total_Centurey AS
(SELECT ID,batter,SUM(batsman_run) as score
FROM ipl_staging
GROUP BY batter,ID
HAVING score >= 100
ORDER BY 3 DESC
)
SELECT batter,COUNT(score)
FROM Total_Centurey
GROUP BY batter
ORDER BY 2 DESC LIMIT 10;

-- There is a problem in the name battingteam column 
-- where there is two categories Rising Pune Supergiant and Rising Pune Supergiants 
-- which is making two differernt category but it is same so i will update it
SELECT * 
FROM ipl_staging
WHERE BattingTeam LIKE 'Rising Pune SupergiantS';

-- changeing from Rising Pune Supergiants to Rising Pune Supergiant in battingteam collom

UPDATE ipl_staging
SET BattingTeam = 'Rising Pune Supergiant'
WHERE BattingTeam LIKE 'Rising Pune Supergiant%';

--  TOP 10 BOWLER WITH MOST WICKETS
SELECT bowler, SUM(isWicketDelivery) wickets
FROM ipl_staging
GROUP BY bowler
ORDER BY SUM(isWicketDelivery) DESC limit 10;

-- BOWLER WITH 5 OR MORE WICKETS IN ONE MATCH
SELECT bowler,SUM(isWicketDelivery) wickets
FROM ipl_staging
GROUP BY bowler,ID
HAVING wickets >= 5
ORDER BY 2 DESC ;

-- BOWLER WHO CONSISDED 30 OR MORE IN SINGLE OVER
SELECT ID,innings,bowler,overs,SUM(total_run) AS `RUNS_PER_OVER`
FROM ipl_staging
GROUP BY overs,ID,innings,bowler
HAVING `RUNS_PER_OVER` >= 30
ORDER BY 5 DESC;

-- IN THE MATCH OF CRICKET THERE ARE ONLY TWO INNINGS BUT IN THE DATA THERE ARE 6 INNINGS SO
-- REMOVING THOSE ROWS WHERE INNINGS IS MORE THA 2 

SELECT COUNT(*)
FROM ipl_staging
WHERE innings = 6;

DELETE FROM ipl_staging
WHERE innings = 6;

-- second day of EDA

-- MATCH PLAYED BY EVERY TEAMS FROM IPL 2008 TO 2023 WITH GROUP BY

SELECT DISTINCT ID,innings,BattingTeam
FROM ipl_staging
GROUP BY ID,innings,BattingTeam;

WITH Match_Played AS
(SELECT DISTINCT ID,innings,BattingTeam
FROM ipl_staging
GROUP BY ID,innings,BattingTeam
)
SELECT BattingTeam,COUNT(BattingTeam) AS MATCH_PLAYED
FROM Match_Played
GROUP BY BattingTeam
ORDER BY 2 DESC;


-- MATCH PLAYED BY EVERY TEAMS FROM IPL 2008 TO 2023 WITH WINDOW FUNCTION
SELECT DISTINCT BattingTeam,
COUNT(*) OVER(PARTITION BY ID,innings) AS MATCH_PLAYED
FROM ipl_staging;

WITH `MATCH` AS
(SELECT DISTINCT ID,innings,BattingTeam,
COUNT(*) OVER(PARTITION BY ID,innings) AS MATCH_PLAYED
FROM ipl_staging
)
SELECT BattingTeam,COUNT(BattingTeam) AS MATCH_P
FROM `MATCH`
GROUP BY BattingTeam
ORDER BY 2 DESC;

-- TOP 10 BATSMAN WITH MOST SIXIS

SELECT batter,total_run,
COUNT(*) OVER(PARTITION BY ID,innings,overs,ballnumber)
FROM ipl_staging
WHERE total_run = 6;

SELECT batter,COUNT(batter)
FROM (SELECT batter,total_run,
COUNT(batter) OVER(PARTITION BY ID,innings,overs,ballnumber)
FROM ipl_staging
WHERE total_run = 6)t
GROUP BY batter
ORDER BY 2 DESC LIMIT 10;

-- TOP 10 BATSMAN WITH MOST FOURS

SELECT batter,total_run,
COUNT(*) OVER(PARTITION BY ID,innings,overs,ballnumber)
FROM ipl_staging
WHERE total_run = 4;

SELECT batter,COUNT(batter)
FROM (SELECT batter,total_run,
COUNT(batter) OVER(PARTITION BY ID,innings,overs,ballnumber)
FROM ipl_staging
WHERE total_run = 4)t
GROUP BY batter
ORDER BY 2 DESC LIMIT 10;

-- THIRD DAY OF EDA

-- RUNS SCORED BY VIRAT KOHLI IN EVERY MATCH AND SUM OF TOTAL RUNS CONSIQUENTLY

SELECT ID,innings,batter,
ROW_NUMBER() OVER(PARTITION BY ID,innings) AS RN,
SUM(total_run) OVER( PARTITION BY ID,innings) AS INNING_SCORE,
SUM(total_run) OVER( ORDER BY ID,innings) AS RUNNIG_SUM
FROM ipl_staging
WHERE batter =  'V Kohli'
ORDER BY ID ;

SELECT ID,innings,INNING_SCORE,RUNNIG_SUM
FROM (SELECT ID,innings,batter,
ROW_NUMBER() OVER(PARTITION BY ID,innings) AS RN,
SUM(total_run) OVER( PARTITION BY ID,innings) AS INNING_SCORE,
SUM(total_run) OVER( ORDER BY ID,innings) AS RUNNIG_SUM
FROM ipl_staging
WHERE batter =  'V Kohli'
ORDER BY ID)t
WHERE RN = 1;


-- ABOVE AVG BATSMAN WITH MININMUM 2000 BALLS PLAYED

SELECT DISTINCT batter,
SUM(total_run) OVER(PARTITION BY batter) runs,
SUM(isWicketDelivery) OVER(PARTITION BY batter) wickets,
COUNT(*) OVER(PARTITION BY BATTER) AS ball_played
FROM ipl_staging;

SELECT batter,runs,wickets,ball_played,runs/wickets batting_avrg
FROM (SELECT DISTINCT batter,
SUM(total_run) OVER(PARTITION BY batter) runs,
SUM(isWicketDelivery) OVER(PARTITION BY batter) wickets,
COUNT(*) OVER(PARTITION BY BATTER) ball_played
FROM ipl_staging)t 
WHERE ball_played >= 2000
ORDER BY 5 DESC;


-- TOP 10 TOTAL SCORE BY TEAMS
SELECT DISTINCT BattingTeam,
SUM(total_run) OVER(PARTITION BY ID,innings,BattingTeam) score
FROM ipl_staging
ORDER BY 2 DESC LIMIT 10;


-- TOTAL SIXES HITS BY ALL TEAMS
SELECT DISTINCT BattingTeam,
COUNT(total_run) OVER(PARTITION BY BattingTeam) sixes_in_ipl
FROM ipl_staging
WHERE total_run = 6
ORDER BY 2 DESC;

-- RUNS SCORED BY VIRAT KOHLI IN POWERPLAY,MIDDLE OVERS,DEATH OVERS

SELECT *,
CASE 
	WHEN overs <= 6 THEN 'POWERPLAY'
    WHEN overs <= 15 THEN 'MIDDLE OVERS'
    ELSE 'DEATH OVERS'
END PhaseByOvers
FROM ipl_staging
WHERE batter =  'V Kohli';

SELECT ID,batter,PhaseByOvers,
SUM(total_run) OVER(PARTITION BY PhaseByOvers) AS RunsByPhase,
ROW_NUMBER() OVER(PARTITION BY PhaseByOvers) AS rn
FROM (SELECT *,
CASE 
	WHEN overs <= 6 THEN 'POWERPLAY'
    WHEN overs <= 15 THEN 'MIDDLE OVERS'
    ELSE 'DEATH OVERS'
END PhaseByOvers
FROM ipl_staging
WHERE batter =  'V Kohli')t;

SELECT ID,batter,PhaseByOvers,RunsByPhase
FROM (SELECT ID,innings,batter,PhaseByOvers,
SUM(total_run) OVER(PARTITION BY PhaseByOvers) AS RunsByPhase,
ROW_NUMBER() OVER(PARTITION BY PhaseByOvers) AS rn
FROM (SELECT *,
CASE 
	WHEN overs <= 6 THEN 'POWERPLAY'
    WHEN overs <= 15 THEN 'MIDDLE OVERS'
    ELSE 'DEATH OVERS'
END PhaseByOvers
FROM ipl_staging
WHERE batter =  'V Kohli')t)t
WHERE rn = 1
ORDER BY 4 DESC;

-- TOP 10 BOWLERS IN THE DEATH OVERS BY WICKETS

SELECT DISTINCT bowler,
SUM(isWicketDelivery) OVER(PARTITION BY bowler) AS WicketsInDeathOvers
FROM ipl_staging
WHERE overs > 15 AND isWicketDelivery = 1
ORDER BY 2 DESC LIMIT 10;


