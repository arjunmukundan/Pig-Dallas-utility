-- Load the data file
data = LOAD '/user/hue/DallasUtilityDemo.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER');
-- Assign field names for 'luxury houses' (containing a sauna OR pool OR spa OR fireplace)
sauna_pool_spa_fire = FOREACH data GENERATE $0 as houseID, $ 4 as neighborhood, $15 as fireplace, $16 as pool, $17 as spa, $18 as sauna, $25 as utility;
utility = FILTER sauna_pool_spa_fire BY (fireplace == 'Y') OR (pool == 'Y') OR (spa == 'Y') OR (sauna == 'Y');
-- Group utility spend by neighborhood and calculate spend per neighborhood for luxury houses
sauna_etc = GROUP utility BY (neighborhood);
luxury_spend = FOREACH sauna_etc GENERATE group as grp1, SUM(sauna_pool_spa_fire.utility) as sum_luxury;
-- Reassign field names
luxury_final = FOREACH luxury_spend GENERATE $0 as neighborhood, $1 as utility;
-- Assign field names for 'non-luxury houses' (not containing sauna, pool, spa or fireplace)
non_utility = FILTER sauna_pool_spa_fire BY ((fireplace == 'N') AND (pool == 'N') AND (spa == 'N') AND (sauna == 'N'));
non_sauna_etc = GROUP non_utility BY (neighborhood);
non_luxury_spend = FOREACH non_sauna_etc GENERATE group as grp2, SUM(sauna_pool_spa_fire.utility) as sum_non_luxury;
--Reassign field names
non_luxury_final = FOREACH non_luxury_spend GENERATE $0 as neighborhood, $1 as utility;
-- Join luxury and non_luxury summations by neighborhood, to compare their utility spending
join_utility = JOIN luxury_final by neighborhood, non_luxury_final by neighborhood;
-- Reassign field names
final = FOREACH join_utility GENERATE $0 as neighborhood, $1 as utility;
-- Display the output
dump final;
